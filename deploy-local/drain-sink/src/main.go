// drain-sink — a pure read-and-discard TCP sink, NOT a matching engine. Used only
// to measure the bot fleet's raw order-GENERATION ceiling with the ack round-trip
// removed: it accepts connections and drains every readable byte as fast as
// possible, but NEVER writes a response. With the receive side always drained, the
// bot fleet's socket writes never backpressure, so the worker's `sent` counter
// reflects pure generation throughput (orders are counted on successful write, no
// ack required; unacked orders just time out in the worker's pending map).
//
// Multi-core: one epoll reactor per allotted core (GOMAXPROCS, cgroup-derived on
// Go 1.25), each on its own SO_REUSEPORT listener, so draining scales across cores.
package main

import (
	"runtime"
	"syscall"
)

const soReusePort = 0xf // SO_REUSEPORT (Linux)

func main() {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	for i := 1; i < n; i++ {
		go reactor()
	}
	reactor()
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

	conns := map[int]bool{}
	events := make([]syscall.EpollEvent, 512)
	buf := make([]byte, 1<<16) // shared discard buffer; contents never used

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
			if !conns[fd] {
				continue
			}
			if !drain(fd, buf) {
				syscall.EpollCtl(ep, syscall.EPOLL_CTL_DEL, fd, nil)
				syscall.Close(fd)
				delete(conns, fd)
			}
		}
	}
}

func addFD(ep, fd int) {
	_ = syscall.EpollCtl(ep, syscall.EPOLL_CTL_ADD, fd, &syscall.EpollEvent{
		Events: syscall.EPOLLIN, Fd: int32(fd),
	})
}

func acceptAll(ep, lfd int, conns map[int]bool) {
	for {
		cfd, _, err := syscall.Accept4(lfd, syscall.SOCK_NONBLOCK)
		if err != nil {
			return // EAGAIN
		}
		_ = syscall.SetsockoptInt(cfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		addFD(ep, cfd)
		conns[cfd] = true
	}
}

// drain reads and discards every available byte. Returns false when the peer
// closed or a hard error occurred. Never writes anything back.
func drain(fd int, buf []byte) bool {
	for {
		nr, err := syscall.Read(fd, buf)
		if err == syscall.EAGAIN {
			return true // drained for now
		}
		if nr == 0 || (err != nil && err != syscall.EINTR) {
			return false // peer closed / hard error
		}
		// discard buf[:nr] and keep reading
	}
}
