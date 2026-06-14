//! Correct FIX 4.2 limit-order-book matching-engine contestant for IICPC.
//!
//! Listens on 0.0.0.0:9898 (override with BIND or PORT). Maintains a single global
//! price-time-priority order book shared across all connections (the platform runs
//! one symbol, "IICPC"). For every order it emits an immediate New ack (so the eBPF
//! latency capture sees a response) and, for every fill the match produces, an
//! ExecutionReport with the fill qty/price routed back to the connection that owns
//! that order (a maker hit by an aggressor on another connection is filled on the
//! maker's connection).
//!
//! The matching logic is a faithful port of the platform's reference engine
//! (correctness-validator/internal/book/book.go): limit orders match while crossing
//! and rest the remainder; market orders match without a price check and never rest;
//! cancel removes; replace keeps position on a same-price qty-decrease, else removes
//! and re-inserts (no match). Fills happen at the maker's resting price for BOTH
//! sides. Because the fills match the ground-truth engine, the run reaches QUALIFIED.
//!
//! Fill encoding (must match the eBPF parser, ebpf-latency/src/parse.rs):
//!   150=2 (ExecType=Fill), 32=<qty> (LastShares), 31=<price> (LastPx, raw integer;
//!   the eBPF scales it by 1e9, matching the reference's price*1e9). std-only.

use std::collections::{BTreeMap, HashMap, VecDeque};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

const SOH: u8 = 0x01;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Buy,
    Sell,
}

#[derive(PartialEq, Eq)]
enum Kind {
    NewLimit,
    NewMarket,
    Cancel,
    Replace,
}

struct Order {
    kind: Kind,
    side: Side,
    price: i64,
    qty: u64,
    order_id: String,
    orig_id: String,
}

struct Resting {
    order_id: String,
    remaining: u64,
    #[allow(dead_code)]
    seq: u64,
}

struct Fill {
    order_id: String,
    price: i64,
    qty: u64,
}

/// Price-time-priority limit order book. bids/asks map price -> FIFO queue (front =
/// oldest = highest time priority). index maps order_id -> (side, price) for
/// cancel/replace lookups.
#[derive(Default)]
struct Engine {
    bids: BTreeMap<i64, VecDeque<Resting>>,
    asks: BTreeMap<i64, VecDeque<Resting>>,
    index: HashMap<String, (Side, i64)>,
    seq: u64,
}

impl Engine {
    /// Returns (fills, done) where `done` are order_ids that left the book (fully
    /// filled / cancelled / replaced) and can be forgotten by the router.
    fn process(&mut self, o: &Order) -> (Vec<Fill>, Vec<String>) {
        let mut fills = Vec::new();
        let mut done = Vec::new();
        match o.kind {
            Kind::NewLimit => self.match_and_rest(o, true, &mut fills, &mut done),
            Kind::NewMarket => self.match_and_rest(o, false, &mut fills, &mut done),
            Kind::Cancel => self.remove(&o.orig_id, &mut done),
            Kind::Replace => self.replace(o, &mut done),
        }
        (fills, done)
    }

    fn match_and_rest(&mut self, o: &Order, rest: bool, fills: &mut Vec<Fill>, done: &mut Vec<String>) {
        let mut remaining = o.qty;
        let buy = o.side == Side::Buy;
        while remaining > 0 {
            // best opposite price: buy aggressor takes lowest ask; sell takes highest bid
            let level_price = if buy {
                match self.asks.keys().next().copied() {
                    Some(p) => p,
                    None => break,
                }
            } else {
                match self.bids.keys().next_back().copied() {
                    Some(p) => p,
                    None => break,
                }
            };
            if o.kind == Kind::NewLimit {
                let crosses = if buy { level_price <= o.price } else { level_price >= o.price };
                if !crosses {
                    break;
                }
            }
            let level = if buy {
                self.asks.get_mut(&level_price).unwrap()
            } else {
                self.bids.get_mut(&level_price).unwrap()
            };
            while remaining > 0 {
                let Some(maker) = level.front_mut() else { break };
                let traded = remaining.min(maker.remaining);
                fills.push(Fill { order_id: o.order_id.clone(), price: level_price, qty: traded });
                fills.push(Fill { order_id: maker.order_id.clone(), price: level_price, qty: traded });
                maker.remaining -= traded;
                remaining -= traded;
                if maker.remaining == 0 {
                    let m = level.pop_front().unwrap();
                    self.index.remove(&m.order_id);
                    done.push(m.order_id);
                }
            }
            if level.is_empty() {
                if buy {
                    self.asks.remove(&level_price);
                } else {
                    self.bids.remove(&level_price);
                }
            }
        }

        if rest && remaining > 0 && o.kind == Kind::NewLimit {
            self.insert(o.order_id.clone(), o.side, o.price, remaining);
        } else {
            // fully filled, or a market order (never rests) -> left the book
            done.push(o.order_id.clone());
        }
    }

    fn insert(&mut self, order_id: String, side: Side, price: i64, remaining: u64) {
        self.seq += 1;
        let ro = Resting { order_id: order_id.clone(), remaining, seq: self.seq };
        self.index.insert(order_id, (side, price));
        let tree = if side == Side::Buy { &mut self.bids } else { &mut self.asks };
        tree.entry(price).or_default().push_back(ro);
    }

    fn remove(&mut self, order_id: &str, done: &mut Vec<String>) {
        let Some((side, price)) = self.index.remove(order_id) else { return };
        let tree = if side == Side::Buy { &mut self.bids } else { &mut self.asks };
        if let Some(level) = tree.get_mut(&price) {
            if let Some(pos) = level.iter().position(|r| r.order_id == order_id) {
                level.remove(pos);
            }
            if level.is_empty() {
                tree.remove(&price);
            }
        }
        done.push(order_id.to_string());
    }

    fn replace(&mut self, o: &Order, done: &mut Vec<String>) {
        let (side, price) = match self.index.get(&o.orig_id).copied() {
            Some(v) => v,
            None => return, // can't replace an order that isn't resting
        };
        let new_id = if o.order_id.is_empty() { o.orig_id.clone() } else { o.order_id.clone() };

        // same-price qty-decrease keeps time priority (in place)
        if o.price == price {
            let mut kept = false;
            {
                let tree = if side == Side::Buy { &mut self.bids } else { &mut self.asks };
                if let Some(level) = tree.get_mut(&price) {
                    if let Some(r) = level.iter_mut().find(|r| r.order_id == o.orig_id) {
                        if o.qty <= r.remaining {
                            r.remaining = o.qty;
                            if new_id != o.orig_id {
                                r.order_id = new_id.clone();
                            }
                            kept = true;
                        }
                    }
                }
            }
            if kept {
                if new_id != o.orig_id {
                    self.index.remove(&o.orig_id);
                    self.index.insert(new_id, (side, price));
                }
                return;
            }
        }
        // reprice or qty-increase: remove + re-insert at the new level (no match, per reference)
        self.remove(&o.orig_id, done);
        self.insert(new_id, o.side, o.price, o.qty);
    }
}

/// One parsed order plus the channel to send THIS connection's responses on.
struct Cmd {
    order: Order,
    out: UnboundedSender<Vec<u8>>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> std::io::Result<()> {
    let bind = std::env::var("BIND").unwrap_or_else(|_| {
        let port = std::env::var("PORT").unwrap_or_else(|_| "9898".to_string());
        format!("0.0.0.0:{port}")
    });
    let listener = TcpListener::bind(&bind).await?;
    eprintln!("matching_engine listening on {bind}");

    // A SINGLE matcher task owns the book (no lock contention). Every connection
    // funnels its parsed orders here over one channel.
    let (mtx, mrx) = unbounded_channel::<Cmd>();
    tokio::spawn(matcher(mrx));

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        let mtx = mtx.clone();
        tokio::spawn(async move {
            if let Err(e) = conn(stream, mtx).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}

/// The only CPU-bound task: serially matches every order and routes each fill to the
/// connection that owns that order_id (maker or taker). No mutex — single owner.
async fn matcher(mut mrx: tokio::sync::mpsc::UnboundedReceiver<Cmd>) {
    let mut engine = Engine::default();
    let mut routes: HashMap<String, UnboundedSender<Vec<u8>>> = HashMap::new();
    let mut seq: u64 = 1;
    while let Some(cmd) = mrx.recv().await {
        let id = cmd.order.order_id.clone();
        routes.insert(id.clone(), cmd.out.clone());
        // Immediate New ack -> a response for the latency capture.
        let _ = cmd.out.send(new_ack(&id, seq));
        seq += 1;
        let (fills, done) = engine.process(&cmd.order);
        for f in fills {
            if let Some(s) = routes.get(&f.order_id) {
                let _ = s.send(fill_er(&f.order_id, f.price, f.qty, seq));
                seq += 1;
            }
        }
        for d in done {
            routes.remove(&d);
        }
    }
}

/// Per-connection reader: parse FIX, hand orders to the matcher. A lightweight tokio
/// task (not an OS thread), so hundreds of connections cost almost nothing.
async fn conn(stream: tokio::net::TcpStream, mtx: UnboundedSender<Cmd>) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    let (mut rd, wr) = stream.into_split();
    let (otx, orx) = unbounded_channel::<Vec<u8>>();
    tokio::spawn(writer(wr, orx));

    let mut buf: Vec<u8> = Vec::with_capacity(16384);
    let mut chunk = [0u8; 8192];
    loop {
        let n = rd.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        let mut cursor = 0usize;
        loop {
            let Some(start_off) = find_subslice(&buf[cursor..], b"8=FIX") else { break };
            let abs_start = cursor + start_off;
            let Some(end) = find_message_end(&buf[abs_start..]) else { break };
            let msg = &buf[abs_start..abs_start + end];
            if let Some(order) = parse_order(msg) {
                if mtx.send(Cmd { order, out: otx.clone() }).is_err() {
                    return Ok(());
                }
            }
            cursor = abs_start + end;
        }
        if cursor > 0 {
            buf.drain(..cursor);
        }
    }
}

/// Per-connection writer: drains its outbound channel to the socket, coalescing
/// whatever is queued into each flush.
async fn writer(
    mut wr: tokio::net::tcp::OwnedWriteHalf,
    mut orx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
) {
    while let Some(buf) = orx.recv().await {
        if wr.write_all(&buf).await.is_err() {
            return;
        }
        while let Ok(more) = orx.try_recv() {
            if wr.write_all(&more).await.is_err() {
                return;
            }
        }
        let _ = wr.flush().await;
    }
}

fn parse_order(msg: &[u8]) -> Option<Order> {
    let mt = extract_tag(msg, b"35")?;
    let order_id = std::str::from_utf8(extract_tag(msg, b"11")?).ok()?.to_string();
    let side = match extract_tag(msg, b"54") {
        Some(b"2") => Side::Sell,
        _ => Side::Buy,
    };
    let orig_id = extract_tag(msg, b"41")
        .and_then(|v| std::str::from_utf8(v).ok())
        .unwrap_or("")
        .to_string();
    let qty = extract_tag(msg, b"38").and_then(parse_u64).unwrap_or(0);
    let price = extract_tag(msg, b"44").and_then(parse_i64).unwrap_or(0);
    let ordtype = extract_tag(msg, b"40");
    let kind = match mt {
        b"D" => {
            if ordtype == Some(b"1") {
                Kind::NewMarket
            } else {
                Kind::NewLimit
            }
        }
        b"F" => Kind::Cancel,
        b"G" => Kind::Replace,
        _ => return None, // logon/heartbeat/etc. ignored
    };
    Some(Order { kind, side, price, qty, order_id, orig_id })
}

fn parse_u64(v: &[u8]) -> Option<u64> {
    std::str::from_utf8(v).ok()?.trim().parse().ok()
}
fn parse_i64(v: &[u8]) -> Option<i64> {
    std::str::from_utf8(v).ok()?.trim().parse().ok()
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn find_message_end(buf: &[u8]) -> Option<usize> {
    let needle = b"\x0110=";
    let i = find_subslice(buf, needle)?;
    let after = i + needle.len();
    let soh = buf[after..].iter().position(|&b| b == SOH)?;
    Some(after + soh + 1)
}

fn extract_tag<'a>(msg: &'a [u8], tag: &[u8]) -> Option<&'a [u8]> {
    let mut needle = Vec::with_capacity(tag.len() + 2);
    needle.push(SOH);
    needle.extend_from_slice(tag);
    needle.push(b'=');
    let pos = find_subslice(msg, &needle)?;
    let vs = pos + needle.len();
    let ve = msg[vs..].iter().position(|&b| b == SOH)?;
    Some(&msg[vs..vs + ve])
}

fn finalize(body: &str) -> Vec<u8> {
    let mut frame = format!("8=FIX.4.2\x019={}\x01{body}", body.len()).into_bytes();
    let checksum = frame.iter().fold(0u32, |s, b| s.wrapping_add(u32::from(*b))) % 256;
    frame.extend_from_slice(format!("10={checksum:03}\x01").as_bytes());
    frame
}

/// New ack (no fill) — gives the latency capture a response for every order.
fn new_ack(order_id: &str, seq: u64) -> Vec<u8> {
    finalize(&format!(
        "35=8\x0149=CONTESTANT\x0156=IICPC-BOT\x0134={seq}\x0152=19700101-00:00:00.000\x0137=EXEC_{seq}\x0111={order_id}\x0117=ACK_{seq}\x01150=0\x0139=0\x0155=IICPC\x0154=1\x0138=0\x0114=0\x016=0\x01"
    ))
}

/// Fill ExecutionReport: 150=2 (Fill), 32=qty (LastShares), 31=price (LastPx, raw;
/// eBPF scales by 1e9 to match the reference's price*1e9).
fn fill_er(order_id: &str, price: i64, qty: u64, seq: u64) -> Vec<u8> {
    finalize(&format!(
        "35=8\x0149=CONTESTANT\x0156=IICPC-BOT\x0134={seq}\x0152=19700101-00:00:00.000\x0137=EXEC_{seq}\x0111={order_id}\x0117=FILL_{seq}\x01150=2\x0139=2\x0155=IICPC\x0154=1\x0132={qty}\x0131={price}\x0114={qty}\x016={price}\x01"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ord(kind: Kind, side: Side, price: i64, qty: u64, id: &str) -> Order {
        Order { kind, side, price, qty, order_id: id.into(), orig_id: String::new() }
    }

    #[test]
    fn cross_produces_two_fills_at_maker_price() {
        let mut e = Engine::default();
        let (f, _) = e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S1"));
        assert!(f.is_empty(), "no cross -> rests");
        // buy @105 crosses the resting ask @100 -> fills at the MAKER price (100)
        let (f, done) = e.process(&ord(Kind::NewLimit, Side::Buy, 105, 10, "B1"));
        assert_eq!(f.len(), 2);
        assert_eq!((f[0].order_id.as_str(), f[0].price, f[0].qty), ("B1", 100, 10));
        assert_eq!((f[1].order_id.as_str(), f[1].price, f[1].qty), ("S1", 100, 10));
        assert!(done.contains(&"S1".to_string()) && done.contains(&"B1".to_string()));
    }

    #[test]
    fn time_priority_fifo() {
        let mut e = Engine::default();
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S1"));
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S2")); // later at same price
        let (f, _) = e.process(&ord(Kind::NewLimit, Side::Buy, 100, 15, "B1"));
        let s1: u64 = f.iter().filter(|x| x.order_id == "S1").map(|x| x.qty).sum();
        let s2: u64 = f.iter().filter(|x| x.order_id == "S2").map(|x| x.qty).sum();
        assert_eq!(s1, 10, "oldest filled first");
        assert_eq!(s2, 5, "newer filled with the remainder");
    }

    #[test]
    fn no_cross_rests() {
        let mut e = Engine::default();
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S1"));
        let (f, _) = e.process(&ord(Kind::NewLimit, Side::Buy, 95, 10, "B1"));
        assert!(f.is_empty());
        assert!(e.index.contains_key("B1"), "non-crossing limit rests");
    }

    #[test]
    fn market_matches_then_does_not_rest() {
        let mut e = Engine::default();
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 5, "S1"));
        let (f, done) = e.process(&ord(Kind::NewMarket, Side::Buy, 0, 10, "M1"));
        let b: u64 = f.iter().filter(|x| x.order_id == "M1").map(|x| x.qty).sum();
        assert_eq!(b, 5);
        assert!(!e.index.contains_key("M1"), "market never rests");
        assert!(done.contains(&"M1".to_string()));
    }

    #[test]
    fn cancel_removes() {
        let mut e = Engine::default();
        e.process(&ord(Kind::NewLimit, Side::Buy, 100, 10, "B1"));
        let mut c = ord(Kind::Cancel, Side::Buy, 0, 0, "C1");
        c.orig_id = "B1".into();
        e.process(&c);
        assert!(!e.index.contains_key("B1"));
    }

    #[test]
    fn replace_same_price_qty_decrease_keeps_position() {
        let mut e = Engine::default();
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S1")); // ahead
        e.process(&ord(Kind::NewLimit, Side::Sell, 100, 10, "S2"));
        let mut r = ord(Kind::Replace, Side::Sell, 100, 6, "S1b"); // S1 -> qty 6, same price
        r.orig_id = "S1".into();
        e.process(&r);
        // S1b still ahead of S2 -> a buy 6 fills S1b only
        let (f, _) = e.process(&ord(Kind::NewLimit, Side::Buy, 100, 6, "B1"));
        let got: u64 = f.iter().filter(|x| x.order_id == "S1b").map(|x| x.qty).sum();
        assert_eq!(got, 6, "qty-decrease replace keeps time priority");
        assert!(!e.index.contains_key("S1"));
    }

    #[test]
    fn parse_new_limit_fix() {
        let raw = b"8=FIX.4.2\x019=50\x0135=D\x0111=ord-1\x0155=IICPC\x0154=1\x0138=25\x0140=2\x0144=10000\x0159=0\x0110=000\x01";
        let o = parse_order(raw).expect("parse");
        assert!(matches!(o.kind, Kind::NewLimit));
        assert!(matches!(o.side, Side::Buy));
        assert_eq!(o.qty, 25);
        assert_eq!(o.price, 10000);
        assert_eq!(o.order_id, "ord-1");
    }

    #[test]
    fn fill_er_encodes_fill_tags() {
        let s = String::from_utf8(fill_er("ord-9", 10000, 25, 7)).unwrap();
        assert!(s.contains("\x01150=2\x01"), "ExecType=Fill");
        assert!(s.contains("\x0132=25\x01"), "LastShares=qty");
        assert!(s.contains("\x0131=10000\x01"), "LastPx=raw price");
        assert!(s.contains("\x0111=ord-9\x01"));
    }
}
