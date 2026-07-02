// A tight, single-threaded epoll matching engine — the "proper HFT contestant".
//
// Why single-threaded epoll: the platform scores correctness by replaying the
// order stream in offline effective_t3 (kernel-veth arrival) order. A live engine
// cannot see effective_t3; the closest it can get is to process orders in the
// order the kernel makes its sockets readable — which is what an epoll loop on a
// single thread does. A goroutine-per-connection server (net/http) instead lets
// the Go scheduler decide processing order, reordering by milliseconds and
// diverging from effective_t3. This engine therefore processes strictly in
// epoll-readiness order (≈ veth arrival order to within microseconds) so its
// market/crossing-limit fills line up with the reference's ordering.
//
// It is a correct price-time-priority CLOB (mirrors correctness-validator's
// reference book): best-price-first, FIFO within a level, trade at the maker's
// resting price, NEW limit crosses then rests, MARKET is IOC, CANCEL/REPLACE act
// on the referenced order, self-trades consumed but not reported. REST/JSON wire.
package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"syscall"
)

// ---- order book (single-threaded: no locks) --------------------------------

type resting struct {
	orderID   string
	price     int64
	remaining uint64
}

var (
	bidLevels = map[int64][]*resting{}
	askLevels = map[int64][]*resting{}
	idx       = map[string]*resting{}
)

func levels(side string) map[int64][]*resting {
	if side == "BUY" {
		return bidLevels
	}
	return askLevels
}
func oppOf(side string) string {
	if side == "BUY" {
		return "SELL"
	}
	return "BUY"
}
func participant(orderID string) string {
	p := strings.Split(orderID, "_")
	if len(p) < 4 {
		return orderID
	}
	return p[len(p)-3]
}

func bestOpposite(aggSide string) (int64, bool) {
	opp := levels(oppOf(aggSide))
	var best int64
	found := false
	for price, q := range opp {
		if len(q) == 0 {
			continue
		}
		if !found {
			best, found = price, true
			continue
		}
		if aggSide == "BUY" && price < best {
			best = price
		}
		if aggSide == "SELL" && price > best {
			best = price
		}
	}
	return best, found
}
func crosses(aggSide string, aggPrice, restPrice int64) bool {
	if aggSide == "BUY" {
		return restPrice <= aggPrice
	}
	return restPrice >= aggPrice
}

func match(aggID, aggSide string, aggPrice int64, qty uint64, isLimit, rest bool) (uint64, int64, bool) {
	remaining := qty
	var rq uint64
	var rp int64
	have := false
	for remaining > 0 {
		price, ok := bestOpposite(aggSide)
		if !ok {
			break
		}
		if isLimit && !crosses(aggSide, aggPrice, price) {
			break
		}
		opp := levels(oppOf(aggSide))
		q := opp[price]
		for len(q) > 0 && remaining > 0 {
			m := q[0]
			traded := remaining
			if m.remaining < traded {
				traded = m.remaining
			}
			if participant(m.orderID) != participant(aggID) {
				rq += traded
				if !have {
					rp, have = price, true
				}
			}
			m.remaining -= traded
			remaining -= traded
			if m.remaining == 0 {
				q = q[1:]
				delete(idx, m.orderID)
			}
		}
		if len(q) == 0 {
			delete(opp, price)
		} else {
			opp[price] = q
		}
	}
	if rest && isLimit && remaining > 0 {
		ro := &resting{orderID: aggID, price: aggPrice, remaining: remaining}
		lv := levels(aggSide)
		lv[aggPrice] = append(lv[aggPrice], ro)
		idx[aggID] = ro
	}
	return rq, rp, have
}

func remove(orderID string) {
	ro, ok := idx[orderID]
	if !ok {
		return
	}
	for _, side := range []map[int64][]*resting{bidLevels, askLevels} {
		q := side[ro.price]
		for i, x := range q {
			if x.orderID == orderID {
				q = append(q[:i], q[i+1:]...)
				if len(q) == 0 {
					delete(side, ro.price)
				} else {
					side[ro.price] = q
				}
				delete(idx, orderID)
				return
			}
		}
	}
	delete(idx, orderID)
}

func replace(newID, origID, side string, price int64, qty uint64) {
	ro, ok := idx[origID]
	if !ok {
		return
	}
	if price == ro.price && qty <= ro.remaining {
		ro.remaining = qty
		if newID != "" && newID != ro.orderID {
			delete(idx, ro.orderID)
			ro.orderID = newID
			idx[newID] = ro
		}
		return
	}
	remove(origID)
	nro := &resting{orderID: newID, price: price, remaining: qty}
	lv := levels(side)
	lv[price] = append(lv[price], nro)
	idx[newID] = nro
}

// ---- request/response ------------------------------------------------------

type req struct {
	ClOrdID     string      `json:"cl_ord_id"`
	OrigClOrdID string      `json:"orig_cl_ord_id"`
	Action      string      `json:"action"`
	OrdType     string      `json:"ord_type"`
	Side        string      `json:"side"`
	Qty         json.Number `json:"qty"`
	Price       json.Number `json:"price"`
}

func toInt(n json.Number) int64 { v, _ := n.Int64(); return v }

// process applies one request (already JSON) to the book and returns the JSON
// response body. Called in strict arrival (epoll-readiness) order.
func process(body []byte) []byte {
	var q req
	_ = json.Unmarshal(body, &q)
	side := strings.ToUpper(q.Side)
	price := toInt(q.Price)
	qty := uint64(toInt(q.Qty))
	execType, fillQty, fillPrice := "0", uint64(0), int64(0)
	switch {
	case strings.EqualFold(q.Action, "CANCEL"):
		remove(q.OrigClOrdID)
	case strings.EqualFold(q.Action, "REPLACE"):
		replace(q.ClOrdID, q.OrigClOrdID, side, price, qty)
	case strings.EqualFold(q.OrdType, "MARKET"):
		if fq, fp, ok := match(q.ClOrdID, side, 0, qty, false, false); ok && fq > 0 {
			execType, fillQty, fillPrice = "2", fq, fp
		}
	default:
		if fq, fp, ok := match(q.ClOrdID, side, price, qty, true, true); ok && fq > 0 {
			execType, fillQty, fillPrice = "2", fq, fp
		}
	}
	// hand-built JSON (hot path; avoids reflection)
	var b strings.Builder
	b.WriteString(`{"cl_ord_id":`)
	cl, _ := json.Marshal(q.ClOrdID)
	b.Write(cl)
	b.WriteString(`,"exec_type":"`)
	b.WriteString(execType)
	b.WriteString(`"`)
	if execType == "2" {
		b.WriteString(`,"fill_qty":`)
		b.WriteString(strconv.FormatUint(fillQty, 10))
		b.WriteString(`,"fill_price":`)
		b.WriteString(strconv.FormatInt(fillPrice, 10))
	}
	b.WriteString("}")
	return []byte(b.String())
}

// ---- epoll HTTP/1.1 server (single thread) ---------------------------------

type conn struct {
	fd  int
	buf []byte // accumulated request bytes
}

func main() {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK, 0)
	if err != nil {
		panic(err)
	}
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Port: 8080}); err != nil {
		panic(err)
	}
	if err := syscall.Listen(fd, 1024); err != nil {
		panic(err)
	}
	ep, err := syscall.EpollCreate1(0)
	if err != nil {
		panic(err)
	}
	addFD(ep, fd)
	conns := map[int]*conn{}
	events := make([]syscall.EpollEvent, 256)

	for {
		n, err := syscall.EpollWait(ep, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			panic(err)
		}
		// Process ready fds in the order epoll returned them — single thread,
		// no scheduler reordering, ≈ kernel arrival order.
		for i := 0; i < n; i++ {
			efd := int(events[i].Fd)
			if efd == fd {
				acceptAll(ep, fd, conns)
				continue
			}
			c := conns[efd]
			if c == nil {
				continue
			}
			if !readAndServe(c) {
				syscall.EpollCtl(ep, syscall.EPOLL_CTL_DEL, efd, nil)
				syscall.Close(efd)
				delete(conns, efd)
			}
		}
	}
}

func addFD(ep, fd int) {
	_ = syscall.EpollCtl(ep, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{
		Events: syscall.EPOLLIN, Fd: int32(fd),
	})
}

func acceptAll(ep, lfd int, conns map[int]*conn) {
	for {
		cfd, _, err := syscall.Accept4(lfd, syscall.SOCK_NONBLOCK)
		if err != nil {
			return // EAGAIN: no more pending
		}
		addFD(ep, cfd)
		conns[cfd] = &conn{fd: cfd, buf: make([]byte, 0, 4096)}
	}
}

// readAndServe drains readable bytes, serves every complete HTTP request in the
// buffer, and returns false when the connection should be closed.
func readAndServe(c *conn) bool {
	tmp := make([]byte, 8192)
	for {
		nr, err := syscall.Read(c.fd, tmp)
		if nr > 0 {
			c.buf = append(c.buf, tmp[:nr]...)
		}
		if err == syscall.EAGAIN {
			break
		}
		if nr == 0 || (err != nil && err != syscall.EINTR) {
			return false // peer closed or hard error
		}
		if nr < len(tmp) {
			break
		}
	}
	// frame + serve every complete request
	for {
		hdrEnd := indexCRLF2(c.buf)
		if hdrEnd < 0 {
			break
		}
		clen := contentLength(c.buf[:hdrEnd])
		total := hdrEnd + 4 + clen
		if len(c.buf) < total {
			break // body incomplete
		}
		body := c.buf[hdrEnd+4 : total]
		respBody := process(body)
		if !writeResponse(c.fd, respBody) {
			return false
		}
		c.buf = c.buf[total:]
	}
	return true
}

func writeResponse(fd int, body []byte) bool {
	resp := append([]byte("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "+strconv.Itoa(len(body))+"\r\n\r\n"), body...)
	for len(resp) > 0 {
		nw, err := syscall.Write(fd, resp)
		if nw > 0 {
			resp = resp[nw:]
		}
		if err == syscall.EAGAIN {
			continue // socket buffer full; spin (rare at these rates)
		}
		if err != nil && err != syscall.EINTR {
			return false
		}
	}
	return true
}

func indexCRLF2(b []byte) int {
	for i := 0; i+3 < len(b); i++ {
		if b[i] == '\r' && b[i+1] == '\n' && b[i+2] == '\r' && b[i+3] == '\n' {
			return i
		}
	}
	return -1
}

func contentLength(hdr []byte) int {
	lower := strings.ToLower(string(hdr))
	i := strings.Index(lower, "content-length:")
	if i < 0 {
		return 0
	}
	rest := string(hdr[i+len("content-length:"):])
	if e := strings.IndexByte(rest, '\r'); e >= 0 {
		rest = rest[:e]
	}
	v, _ := strconv.Atoi(strings.TrimSpace(rest))
	return v
}
