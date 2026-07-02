// FIX 4.2 network front-end for the Orderbook matching engine (IICPC contestant glue).
//
// The cloned engine (Orderbook in orderbook.{h,cpp}) is a pure in-process library:
// it matches via direct function calls and has no network interface. The IICPC
// platform drives contestants over TCP with FIX 4.2 and captures latency at the
// sandbox NIC (eBPF on port 9898). This file is the adapter that makes the library
// benchmarkable: it terminates FIX on :9898, translates each message into an
// Orderbook call, and encodes the resulting trades back as FIX ExecutionReports
// the way the platform's eBPF parser and correctness validator expect.
//
// Design (mirrors the platform reference, e2e/contestant-matching-engine):
//   * ONE matcher thread owns the Orderbook + all id/route maps -> zero lock
//     contention on the hot path, deterministic single-threaded matching.
//   * Per connection: one reader thread (parse FIX -> push command) and one
//     writer thread (drain this connection's outbound queue -> socket).
//   * A single global book is shared across all connections (the platform runs
//     one symbol, "IICPC"); a maker resting from connection A that is hit by an
//     aggressor on connection B is filled back on connection A.
//
// id model: the bot's ClOrdID (FIX tag 11) is an arbitrary string; the Orderbook
// keys on uint64 OrderId. The glue assigns a monotonic uint64 per new ClOrdID and
// maps both ways, plus uint64 -> owning connection for fill routing.
//
// Fill encoding (must match ebpf-latency/src/parse.rs and the validator):
//   New ack:  35=8, 150=0 (New), 39=0     -> a response for every order so the
//             latency capture sees one even for orders that only rest.
//   Fill:     35=8, 150=2 (Fill), 32=<qty> (LastShares), 31=<price> (LastPx, raw
//             integer; the eBPF scales by 1e9 to match the reference price*1e9).
//
// FIDELITY NOTE: this glue reports exactly what the engine computes. The engine
// has two behaviours that DIFFER from the platform's ground-truth book, so they
// will (correctly) cost correctness score — they are properties of this engine,
// not bugs in the glue:
//   1. It fills each side at that order's OWN limit price (orderbook.cpp uses
//      bid->GetPrice() and ask->GetPrice()), not the maker's resting price. The
//      reference fills both sides at the maker price.
//   2. ModifyOrder is cancel+re-add, so a replace always loses time priority
//      (the reference keeps priority on a same-price quantity decrease).
//
// std + POSIX sockets + pthreads only — no external deps, builds in the platform
// build-worker (ubuntu:22.04).

#include "orderbook.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

constexpr char SOH = '\x01';

// ----------------------------- FIX encoding --------------------------------

// finalize wraps a body in the FIX 4.2 envelope: 8=FIX.4.2 | 9=<bodylen> | body |
// 10=<checksum>. Checksum = sum of all bytes up to (not including) the 10= tag,
// modulo 256, three digits.
std::string finalize(const std::string& body) {
    std::string frame = "8=FIX.4.2" + std::string(1, SOH) + "9=" +
                        std::to_string(body.size()) + std::string(1, SOH) + body;
    uint32_t sum = 0;
    for (unsigned char c : frame) sum += c;
    sum %= 256;
    char cks[8];
    std::snprintf(cks, sizeof(cks), "10=%03u", sum);
    frame += std::string(1, SOH) + cks + std::string(1, SOH);
    return frame;
}

// new_ack: ExecutionReport, OrdStatus=New, ExecType=New (150=0/39=0). Echoes the
// ClOrdID so the load generator can match the response to its sent order.
std::string new_ack(const std::string& clord_id, uint64_t seq) {
    std::string s = std::to_string(seq);
    std::string body =
        "35=8" + std::string(1, SOH) +
        "49=CONTESTANT" + std::string(1, SOH) +
        "56=IICPC-BOT" + std::string(1, SOH) +
        "34=" + s + std::string(1, SOH) +
        "52=19700101-00:00:00.000" + std::string(1, SOH) +
        "37=EXEC_" + s + std::string(1, SOH) +
        "11=" + clord_id + std::string(1, SOH) +
        "17=ACK_" + s + std::string(1, SOH) +
        "150=0" + std::string(1, SOH) +
        "39=0" + std::string(1, SOH) +
        "55=IICPC" + std::string(1, SOH) +
        "54=1" + std::string(1, SOH) +
        "38=0" + std::string(1, SOH) +
        "14=0" + std::string(1, SOH) +
        "6=0" + std::string(1, SOH);
    return finalize(body);
}

// fill_er: ExecutionReport, ExecType=Fill (150=2), with LastShares (32) and LastPx
// (31, raw integer). price is the engine's reported fill price for this side.
// liquidity is FIX LastLiquidityInd (tag 851): 2 = taker (this fill's order was the
// aggressor that crossed the book), 1 = maker (it was resting). The benchmark samples
// matching latency on taker fills only, so this tag must be correct per side.
std::string fill_er(const std::string& clord_id, long long price, uint64_t qty, uint64_t seq,
                    int liquidity) {
    std::string s = std::to_string(seq);
    std::string p = std::to_string(price);
    std::string q = std::to_string(qty);
    std::string body =
        "35=8" + std::string(1, SOH) +
        "49=CONTESTANT" + std::string(1, SOH) +
        "56=IICPC-BOT" + std::string(1, SOH) +
        "34=" + s + std::string(1, SOH) +
        "52=19700101-00:00:00.000" + std::string(1, SOH) +
        "37=EXEC_" + s + std::string(1, SOH) +
        "11=" + clord_id + std::string(1, SOH) +
        "17=FILL_" + s + std::string(1, SOH) +
        "150=2" + std::string(1, SOH) +
        "39=2" + std::string(1, SOH) +
        "55=IICPC" + std::string(1, SOH) +
        "54=1" + std::string(1, SOH) +
        "32=" + q + std::string(1, SOH) +
        "31=" + p + std::string(1, SOH) +
        "14=" + q + std::string(1, SOH) +
        "6=" + p + std::string(1, SOH) +
        "851=" + std::to_string(liquidity) + std::string(1, SOH);
    return finalize(body);
}

// ----------------------------- FIX parsing ---------------------------------

size_t find_sub(std::string_view hay, std::string_view needle, size_t from) {
    if (needle.empty() || from > hay.size()) return std::string_view::npos;
    return hay.find(needle, from);
}

// extract_tag returns the SOH-delimited value of "<SOH><tag>=", or nullopt.
std::optional<std::string_view> extract_tag(std::string_view msg, std::string_view tag) {
    std::string needle = std::string(1, SOH) + std::string(tag) + "=";
    size_t pos = msg.find(needle);
    if (pos == std::string_view::npos) {
        // Tag at the very start (after 8=FIX...9=...) still begins with SOH in our
        // framing, so the leading-SOH form covers every real field.
        return std::nullopt;
    }
    size_t vs = pos + needle.size();
    size_t ve = msg.find(SOH, vs);
    if (ve == std::string_view::npos) return std::nullopt;
    return msg.substr(vs, ve - vs);
}

// find_message_end returns the index one past the trailing SOH of the checksum
// field (\x0110=NNN\x01), or npos if the message is not fully buffered yet.
size_t find_message_end(std::string_view buf, size_t start) {
    std::string needle = std::string(1, SOH) + "10=";
    size_t i = buf.find(needle, start);
    if (i == std::string_view::npos) return std::string_view::npos;
    size_t after = i + needle.size();
    size_t soh = buf.find(SOH, after);
    if (soh == std::string_view::npos) return std::string_view::npos;
    return soh + 1;
}

enum class Kind { NewLimit, NewMarket, Cancel, Replace };

struct ParsedOrder {
    Kind kind;
    bool buy;
    long long price;
    uint64_t qty;
    std::string clord_id;  // tag 11
    std::string orig_id;   // tag 41 (cancel/replace target)
};

std::optional<ParsedOrder> parse_order(std::string_view msg) {
    auto mt = extract_tag(msg, "35");
    auto cl = extract_tag(msg, "11");
    if (!mt || !cl) return std::nullopt;

    ParsedOrder o;
    o.clord_id = std::string(*cl);
    o.buy = !(extract_tag(msg, "54") == std::optional<std::string_view>("2"));
    if (auto orig = extract_tag(msg, "41")) o.orig_id = std::string(*orig);
    o.qty = 0;
    if (auto q = extract_tag(msg, "38")) {
        try { o.qty = std::stoull(std::string(*q)); } catch (...) { o.qty = 0; }
    }
    o.price = 0;
    if (auto p = extract_tag(msg, "44")) {
        try { o.price = std::stoll(std::string(*p)); } catch (...) { o.price = 0; }
    }
    auto ordtype = extract_tag(msg, "40");

    if (*mt == "D") {
        o.kind = (ordtype == std::optional<std::string_view>("1")) ? Kind::NewMarket : Kind::NewLimit;
    } else if (*mt == "F") {
        o.kind = Kind::Cancel;
    } else if (*mt == "G") {
        o.kind = Kind::Replace;
    } else {
        return std::nullopt;  // logon/heartbeat/etc. ignored
    }
    return o;
}

// ------------------------- per-connection output ---------------------------

// ConnOut is one connection's outbound side: a queue drained by its writer thread.
// Shared via shared_ptr so the matcher thread can route fills here even after the
// reader has moved on.
struct ConnOut {
    int fd;
    std::mutex m;
    std::condition_variable cv;
    std::deque<std::string> q;
    bool closed = false;

    explicit ConnOut(int fd_) : fd(fd_) {}

    void push(std::string s) {
        {
            std::lock_guard<std::mutex> lk(m);
            if (closed) return;
            q.push_back(std::move(s));
        }
        cv.notify_one();
    }

    void close() {
        {
            std::lock_guard<std::mutex> lk(m);
            closed = true;
        }
        cv.notify_one();
    }
};

bool write_all(int fd, const char* data, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = ::send(fd, data + off, len - off, MSG_NOSIGNAL);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR)) continue;
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

// writer_thread coalesces everything currently queued into each send() so a burst
// of fills crosses the wire in one syscall. TCP_NODELAY (set on accept) means each
// send flushes immediately rather than waiting for Nagle.
void writer_thread(std::shared_ptr<ConnOut> out) {
    std::string batch;
    for (;;) {
        std::deque<std::string> local;
        {
            std::unique_lock<std::mutex> lk(out->m);
            out->cv.wait(lk, [&] { return out->closed || !out->q.empty(); });
            if (out->q.empty() && out->closed) break;
            local.swap(out->q);
        }
        batch.clear();
        for (auto& s : local) batch += s;
        if (!batch.empty() && !write_all(out->fd, batch.data(), batch.size())) {
            out->close();
            break;
        }
    }
}

// ------------------------------- matcher -----------------------------------

struct Command {
    ParsedOrder order;
    std::shared_ptr<ConnOut> out;
};

// Single-producer-agnostic / single-consumer command queue feeding the matcher.
struct MatcherQueue {
    std::mutex m;
    std::condition_variable cv;
    std::deque<Command> q;

    void push(Command c) {
        {
            std::lock_guard<std::mutex> lk(m);
            q.push_back(std::move(c));
        }
        cv.notify_one();
    }

    Command pop() {
        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&] { return !q.empty(); });
        Command c = std::move(q.front());
        q.pop_front();
        return c;
    }
};

class Matcher {
public:
    void run(MatcherQueue& mq) {
        for (;;) {
            Command cmd = mq.pop();
            handle(cmd);
        }
    }

private:
    Orderbook book_;
    uint64_t next_id_ = 1;
    uint64_t seq_ = 1;
    std::unordered_map<std::string, uint64_t> id_of_;   // ClOrdID -> internal
    std::unordered_map<uint64_t, std::string> str_of_;  // internal -> ClOrdID
    std::unordered_map<uint64_t, std::shared_ptr<ConnOut>> route_;  // internal -> conn
    std::unordered_map<uint64_t, uint64_t> remaining_;  // internal -> remaining qty in book

    void forget(uint64_t id) {
        auto it = str_of_.find(id);
        if (it != str_of_.end()) {
            id_of_.erase(it->second);
            str_of_.erase(it);
        }
        route_.erase(id);
        remaining_.erase(id);
    }

    // emit_fills converts engine Trades into per-side FIX fills routed to each
    // order's owning connection, and retires orders whose remaining hits zero.
    // aggressor is the internal id of the incoming order that triggered this match.
    // The side of each trade whose orderId == aggressor is the taker (851=2); the
    // resting counterparty is the maker (851=1).
    void emit_fills(const Trades& trades, OrderId aggressor) {
        for (const auto& t : trades) {
            const auto& bid = t.GetBidTrade();
            const auto& ask = t.GetAskTrade();
            send_fill(bid.orderId_, bid.price_, bid.quantity_,
                      bid.orderId_ == aggressor ? 2 : 1);
            send_fill(ask.orderId_, ask.price_, ask.quantity_,
                      ask.orderId_ == aggressor ? 2 : 1);
        }
    }

    void send_fill(OrderId id, Price price, Quantity qty, int liquidity) {
        auto sit = str_of_.find(id);
        auto rit = route_.find(id);
        if (sit != str_of_.end() && rit != route_.end()) {
            rit->second->push(fill_er(sit->second, static_cast<long long>(price),
                                      static_cast<uint64_t>(qty), seq_++, liquidity));
        }
        auto remit = remaining_.find(id);
        if (remit != remaining_.end()) {
            if (qty >= remit->second) {
                forget(id);  // fully filled -> left the book
            } else {
                remit->second -= qty;
            }
        }
    }

    void handle(const Command& cmd) {
        const ParsedOrder& o = cmd.order;

        // Immediate New ack -> a response for the latency capture, for every message.
        cmd.out->push(new_ack(o.clord_id, seq_++));

        Side side = o.buy ? Side::Buy : Side::Sell;

        switch (o.kind) {
            case Kind::NewLimit: {
                uint64_t id = next_id_++;
                id_of_[o.clord_id] = id;
                str_of_[id] = o.clord_id;
                route_[id] = cmd.out;
                remaining_[id] = o.qty;
                auto order = std::make_shared<Order>(
                    OrderType::GoodTillCancel, id, side,
                    static_cast<Price>(o.price), static_cast<Quantity>(o.qty));
                emit_fills(book_.AddOrder(order), id);
                break;
            }
            case Kind::NewMarket: {
                uint64_t id = next_id_++;
                id_of_[o.clord_id] = id;
                str_of_[id] = o.clord_id;
                route_[id] = cmd.out;
                remaining_[id] = o.qty;
                // Market ctor (price 0); the engine converts to GTC at the worst
                // price and matches. (NOTE: this engine lets an unfilled market
                // remainder REST, unlike the reference where market never rests.)
                auto order = std::make_shared<Order>(id, side, static_cast<Quantity>(o.qty));
                emit_fills(book_.AddOrder(order), id);
                break;
            }
            case Kind::Cancel: {
                auto it = id_of_.find(o.orig_id);
                if (it != id_of_.end()) {
                    uint64_t id = it->second;
                    book_.CancelOrder(id);
                    forget(id);
                }
                break;
            }
            case Kind::Replace: {
                auto it = id_of_.find(o.orig_id);
                if (it == id_of_.end()) break;  // can't replace an unknown order
                uint64_t id = it->second;
                // ModifyOrder reuses the internal id (cancel + re-add). Re-key the
                // string maps so this order is now known by the replace's new
                // ClOrdID -> subsequent fills report the new id (matches the FIX
                // replace contract). The engine itself loses time priority here.
                Trades trades = book_.ModifyOrder(
                    OrderModify(id, side, static_cast<Price>(o.price),
                                static_cast<Quantity>(o.qty)));
                id_of_.erase(o.orig_id);
                id_of_[o.clord_id] = id;
                str_of_[id] = o.clord_id;
                route_[id] = cmd.out;
                remaining_[id] = o.qty;
                emit_fills(trades, id);
                break;
            }
        }
    }
};

// ---------------------------- connection I/O -------------------------------

void reader_thread(int fd, MatcherQueue* mq) {
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));  // Nagle off

    auto out = std::make_shared<ConnOut>(fd);
    std::thread(writer_thread, out).detach();

    std::string buf;
    buf.reserve(1 << 16);
    char chunk[8192];

    for (;;) {
        ssize_t n = ::recv(fd, chunk, sizeof(chunk), 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            break;  // peer closed or error
        }
        buf.append(chunk, static_cast<size_t>(n));

        size_t cursor = 0;
        for (;;) {
            size_t start = find_sub(buf, "8=FIX", cursor);
            if (start == std::string::npos) break;
            size_t end = find_message_end(buf, start);
            if (end == std::string::npos) break;  // incomplete trailer
            std::string_view msg(buf.data() + start, end - start);
            if (auto po = parse_order(msg)) {
                mq->push(Command{std::move(*po), out});
            }
            cursor = end;
        }
        if (cursor > 0) buf.erase(0, cursor);
        if (buf.size() > (4u << 20)) buf.clear();  // runaway guard
    }

    out->close();
    ::close(fd);
}

}  // namespace

int main() {
    // BIND overrides everything; else 0.0.0.0:$PORT, default 9898 (the eBPF-filtered
    // FIX port). Matches the platform reference contestant.
    std::string host = "0.0.0.0";
    int port = 9898;
    if (const char* p = std::getenv("PORT")) {
        try { port = std::stoi(p); } catch (...) {}
    }
    if (const char* b = std::getenv("BIND")) {
        std::string s(b);
        auto colon = s.rfind(':');
        if (colon != std::string::npos) {
            host = s.substr(0, colon);
            try { port = std::stoi(s.substr(colon + 1)); } catch (...) {}
        }
    }

    int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) {
        std::perror("socket");
        return 1;
    }
    int one = 1;
    ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (host == "0.0.0.0") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        addr.sin_addr.s_addr = INADDR_ANY;
    }

    if (::bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::perror("bind");
        return 1;
    }
    if (::listen(lfd, 1024) < 0) {
        std::perror("listen");
        return 1;
    }
    std::fprintf(stderr, "matching_engine (cpp orderbook + FIX glue) listening on %s:%d\n",
                 host.c_str(), port);

    static MatcherQueue mq;
    static Matcher matcher;
    std::thread([&] { matcher.run(mq); }).detach();

    for (;;) {
        int cfd = ::accept(lfd, nullptr, nullptr);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            continue;
        }
        std::thread(reader_thread, cfd, &mq).detach();
    }
}
