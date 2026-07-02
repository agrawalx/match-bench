# IICPC FIX adapter for the C++ Orderbook

Wraps the pure-library `Orderbook` engine in a FIX 4.2 TCP server so the IICPC
platform can benchmark it. Added/changed files only — engine matching logic is
untouched.

## What was added/changed
- `src/fix_server.cpp` — FIX 4.2 front-end (NEW). Listens `:9898`, TCP_NODELAY,
  single matcher thread owns the book, per-connection reader+writer threads,
  ClOrdID(string)↔OrderId(uint64) mapping, cross-connection fill routing,
  New-ack-per-order + Fill ExecutionReports in the eBPF/validator wire format.
- `CMakeLists.txt` — added `matching_engine` executable target (links
  `orderbook_lib` + Threads, `-O3`); gated googletest/tests behind
  `-DORDERBOOK_BUILD_TESTS=ON` (OFF by default) so the platform's offline
  `cmake -B out` configure does not fetch over the network.
- `include/order.h` — replaced `<format>`/`std::format` with `std::to_string`
  concat. Required: the platform build image is `ubuntu:22.04` (gcc-11), whose
  libstdc++ has no `<format>`. Only two error-message strings change; matching is
  identical.
- `benchmark.yaml` — submission manifest (protocol FIX, cpp, port 9898, cmake
  target `matching_engine`).

## TCP / perf
- Nagle disabled (`TCP_NODELAY`) on every accepted socket.
- `SO_REUSEADDR` on the listener; writer coalesces queued fills into one `send`.
- Single-owner book (no mutex on the match path), `-O3` Release build.

## Behaviour vs the platform's reference book (expect correctness < 1.0)
This adapter reports exactly what the engine computes. Two engine properties
differ from ground truth and will cost correctness score — they are real
properties of *this* engine, not adapter bugs:
1. **Per-side own-price fills.** `orderbook.cpp` records `bid->GetPrice()` /
   `ask->GetPrice()`, so an aggressor fills at its own limit price, not the
   maker's resting price. (Smoke test: buy@105 vs resting sell@100 → buy filled
   @105, sell @100; the reference would fill both @100.)
2. **Replace loses time priority.** `ModifyOrder` is cancel+re-add; the reference
   keeps priority on a same-price quantity decrease.
Also: an unfilled **market** remainder rests here (reference markets never rest).

## Build locally
    cmake -B out -DCMAKE_BUILD_TYPE=Release
    cmake --build out --target matching_engine -j
    ./out/matching_engine          # PORT=NNNN or BIND=host:port to override

## Submit
Zip the repo root (must contain `benchmark.yaml` + `CMakeLists.txt` + `src/` +
`include/`) and upload to the submission-api:

    cd <repo root>
    zip -r ../cpp-orderbook.zip . -x 'out/*' '.git/*'

The build-worker generates the cpp Dockerfile, runs
`cmake -B out -DCMAKE_BUILD_TYPE=Release && cmake --build out --target matching_engine`,
and copies `/build/out/matching_engine`.
