# iicpc-ebpf-latency

Rust userspace publisher for algo-pod wire-to-wire timestamps.

The service loads an Aya-compatible object, attaches XDP ingress program
`iicpc_xdp_ingress` and tc egress program `iicpc_tc_egress` to `EBPF_IFACE`,
drains ring-buffer map `EVENTS`, and publishes MessagePack `OrderAckedBatch`
records to `orders.acked`.

In Kubernetes production mode, set `EBPF_NETNS_PATH` to the contestant pod's
network namespace path, for example `/proc/<algo-pid>/ns/net`, and set
`EBPF_IFACE=eth0`. The loader temporarily enters that network namespace while
attaching programs, so XDP ingress sees requests entering the pod and tc egress
sees responses leaving the pod.

This follows `MEASUREMENT_AND_FAIRNESS.md`: ingress stores
`t3_xdp_ingress_ns` in an LRU in-flight map keyed by order id, egress matches
the response carrying the same order id, captures `t7_xdp_egress_ns`, and
emits `pod_service_time_ns = max(0, t7 - t3)`.

Supported wire formats:

- FIX on TCP port `9898`: request tag `11` from `35=D/F/G`, response tag `11`
  from `35=8`. Response metadata is parsed from tags `150`/`39`, `32`, `31`,
  and optional `41`.
- REST on TCP port `8080`: JSON field `"cl_ord_id"` in the HTTP request and
  response body. Response metadata is parsed from `"exec_type"`/`"ord_status"`,
  `"fill_qty"`, `"fill_price"`, and optional `"orig_cl_ord_id"`.
- WebSocket on TCP port `8080`: JSON field `"cl_ord_id"` in text or binary
  frames, with the same response metadata fields as REST. Client-to-server
  masked frames are unmasked in the parser.

Required environment:

- `SESSION_ID`
- `CONTESTANT_ID`
- `EBPF_IFACE`
- `EBPF_OBJECT_PATH`
- `KAFKA_BROKERS`

Optional environment:

- `ORDERS_ACKED_TOPIC` default `orders.acked`
- `EBPF_NETNS_PATH` optional target network namespace for pod-side attachment
- `EBPF_XDP_INGRESS_PROGRAM` default `iicpc_xdp_ingress`
- `EBPF_TC_EGRESS_PROGRAM` default `iicpc_tc_egress`
- `EBPF_RINGBUF_MAP` default `EVENTS`
- `EBPF_FLUSH_INTERVAL_MS` default `5`
- `EBPF_BATCH_SIZE` default `4096`

The eBPF program must write this fixed event ABI:

```c
struct event {
    __u64 t3_xdp_ingress_ns;
    __u64 t7_xdp_egress_ns;
    __u64 pod_service_time_ns;
    __u64 fill_qty;
    __u64 fill_price; // fixed-point, scale = 1_000_000_000
    __u32 src_ip;
    __u32 tcp_seq;
    __u32 retransmission_count;
    __u16 src_port;
    __u16 order_id_len;
    __u16 exec_type_len;
    __u16 orig_order_id_len;
    __u16 flags; // bit 0: reordering_detected
    __u16 _pad;
    __u8  order_id[96];
    __u8  exec_type[16];
    __u8  orig_order_id[96];
};
```

Both kernel stamps must use `bpf_ktime_get_real_ns()` so they remain in the
same CLOCK_REALTIME domain as bot fleet `t0/t1/r9` telemetry.
