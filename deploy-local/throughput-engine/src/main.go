// throughput-engine — a deliberately TRIVIAL, maximally-fast "ack everything"
// HTTP server, used ONLY to measure the bot fleet's raw order-generation ceiling.
//
// It is NOT a correct matching engine and makes no attempt to be one: for every
// HTTP request it reads, it immediately writes back a minimal acknowledgement that
// echoes the request's cl_ord_id (the only field the bot fleet matches on to count
// an order as "acked"). No order book, no parsing beyond locating cl_ord_id, no
// allocation on the hot path beyond the response bytes.
//
// Design goals (so the ENGINE is never the bottleneck — we want to measure the
// FLEET, not this):
//   - Multi-core: one epoll loop per CPU, each on its own SO_REUSEPORT listener,
//     so the kernel load-balances connections across all cores.
//   - Aggressive draining: each readable socket is drained fully every wakeup, so
//     the bot fleet's TCP writes never block (no TCP backpressure).
//   - Edge-light: HTTP/1.1 keep-alive, responses are pre-sized, cl_ord_id is found
//     by a byte scan rather than a JSON unmarshal.
package main

import (
	"runtime"
	"strconv"
	"syscall"
)

const soReusePort = 0xf // SO_REUSEPORT (Linux); not exported by the syscall pkg.

func main() {
	// One epoll reactor per SCHEDULABLE core. Use GOMAXPROCS (which Go 1.25 derives
	// from the cgroup CPU quota) rather than NumCPU (the host's core count): on a
	// CPU-limited pod, NumCPU returns the node's cores, so we'd spawn far more
	// OS-locked reactor threads than the pod's quota and the kernel CFS-throttles
	// them — low average CPU but high latency, which backpressures the bot fleet.
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	for i := 1; i < n; i++ {
		go reactor()
	}
	reactor() // run one on the main goroutine too
}

func reactor() {
	runtime.LockOSThread()

	lfd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK, 0)
	if err != nil {
		panic(err)
	}
	_ = syscall.SetsockoptInt(lfd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	_ = syscall.SetsockoptInt(lfd, syscall.SOL_SOCKET, soReusePort, 1)
	if err := syscall.Bind(lfd, &syscall.SockaddrInet4{Port: 8080}); err != nil {
		panic(err)
	}
	if err := syscall.Listen(lfd, 1024); err != nil {
		panic(err)
	}
	ep, err := syscall.EpollCreate1(0)
	if err != nil {
		panic(err)
	}
	addFD(ep, lfd)

	conns := map[int]*conn{}
	events := make([]syscall.EpollEvent, 512)
	for {
		nev, err := syscall.EpollWait(ep, events, -1)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			panic(err)
		}
		for i := 0; i < nev; i++ {
			fd := int(events[i].Fd)
			if fd == lfd {
				acceptAll(ep, lfd, conns)
				continue
			}
			c := conns[fd]
			if c == nil {
				continue
			}
			if !c.readAndAck() {
				syscall.EpollCtl(ep, syscall.EPOLL_CTL_DEL, fd, nil)
				syscall.Close(fd)
				delete(conns, fd)
			}
		}
	}
}

type conn struct {
	fd  int
	buf []byte
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
			return // EAGAIN
		}
		_ = syscall.SetsockoptInt(cfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		addFD(ep, cfd)
		conns[cfd] = &conn{fd: cfd, buf: make([]byte, 0, 8192)}
	}
}

// readAndAck drains all readable bytes, frames every complete HTTP/1.1 request in
// the buffer, and writes one minimal ack per request. Returns false to close.
func (c *conn) readAndAck() bool {
	tmp := make([]byte, 65536)
	for {
		nr, err := syscall.Read(c.fd, tmp)
		if nr > 0 {
			c.buf = append(c.buf, tmp[:nr]...)
		}
		if err == syscall.EAGAIN {
			break
		}
		if nr == 0 || (err != nil && err != syscall.EINTR) {
			return false
		}
		if nr < len(tmp) {
			break
		}
	}

	var out []byte
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
		out = appendAck(out, clOrdID(body))
		c.buf = c.buf[total:]
	}
	if len(out) > 0 {
		return writeAll(c.fd, out)
	}
	return true
}

// appendAck appends a complete HTTP/1.1 200 response carrying {"cl_ord_id":"<id>",
// "exec_type":"0"} for the given id. Responses are batched into one write per wakeup.
func appendAck(out []byte, id []byte) []byte {
	body := make([]byte, 0, len(id)+40)
	body = append(body, `{"cl_ord_id":"`...)
	body = append(body, id...)
	body = append(body, `","exec_type":"0"}`...)

	out = append(out, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "...)
	out = append(out, strconv.Itoa(len(body))...)
	out = append(out, "\r\n\r\n"...)
	out = append(out, body...)
	return out
}

// clOrdID locates the "cl_ord_id":"..." value in a JSON body by byte scan (no
// unmarshal). Returns an empty slice if absent (still acked, just unmatched).
func clOrdID(body []byte) []byte {
	key := []byte(`"cl_ord_id"`)
	i := indexOf(body, key)
	if i < 0 {
		return nil
	}
	j := i + len(key)
	// skip spaces and the colon
	for j < len(body) && (body[j] == ' ' || body[j] == ':' || body[j] == '\t') {
		j++
	}
	if j >= len(body) || body[j] != '"' {
		return nil
	}
	j++ // past opening quote
	start := j
	for j < len(body) && body[j] != '"' {
		j++
	}
	return body[start:j]
}

func writeAll(fd int, b []byte) bool {
	for len(b) > 0 {
		nw, err := syscall.Write(fd, b)
		if nw > 0 {
			b = b[nw:]
		}
		if err == syscall.EAGAIN {
			continue
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
	i := indexOfFold(hdr, []byte("content-length:"))
	if i < 0 {
		return 0
	}
	j := i + len("content-length:")
	for j < len(hdr) && (hdr[j] == ' ' || hdr[j] == '\t') {
		j++
	}
	n := 0
	for j < len(hdr) && hdr[j] >= '0' && hdr[j] <= '9' {
		n = n*10 + int(hdr[j]-'0')
		j++
	}
	return n
}

func indexOf(h, n []byte) int {
	if len(n) == 0 || len(h) < len(n) {
		return -1
	}
	for i := 0; i+len(n) <= len(h); i++ {
		k := 0
		for k < len(n) && h[i+k] == n[k] {
			k++
		}
		if k == len(n) {
			return i
		}
	}
	return -1
}

// indexOfFold is indexOf with ASCII case-insensitive matching (for header names).
func indexOfFold(h, n []byte) int {
	if len(n) == 0 || len(h) < len(n) {
		return -1
	}
	for i := 0; i+len(n) <= len(h); i++ {
		k := 0
		for k < len(n) && lower(h[i+k]) == lower(n[k]) {
			k++
		}
		if k == len(n) {
			return i
		}
	}
	return -1
}

func lower(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + 32
	}
	return b
}
