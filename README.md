# IICPC

> **Fair, repeatable benchmarking for high-frequency trading algorithms.**

IICPC lets participants upload trading algorithms, runs each submission in an isolated environment, sends every participant the same deterministic market workload, measures latency outside the participant's code, validates correctness, and publishes scores on a live leaderboard.

The platform is built for one principle: 

> "**A benchmark is only useful if nobody can game the measurement.**"

## Table Of Contents

- [Overview](#overview)
- [Benchmark Snapshot](#benchmark-snapshot)
- [Key Features](#key-features)
- [How It Works](#how-it-works)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Repository Layout](#repository-layout)
- [Getting Started](#getting-started)
- [Testing](#testing)
- [Deployment](#deployment)
- [Architecture Document](#architecture-document)
- [Demo Video](#demo-video)
- [Design Notes](#design-notes)

## Overview

IICPC is a multi-service benchmark platform for evaluating untrusted trading algorithms. A submitted algorithm is built into a container, deployed into a locked-down Kubernetes pod, driven by deterministic benchmark traffic, measured at the Linux network boundary, checked against a correctness model, and scored.

## Benchmark Snapshot

Initial local benchmarking shows the load-generation path is not the bottleneck for realistic contest-scale runs.

| Environment / assumption | Observed or projected throughput |
|---|---:|
| Laptop, single core test | ~68,000 orders/sec |
| CPU usage during local single-core test | ~0.74% of one core |
| EKS paid worker node assumption | 4 cores |
| Projected throughput per worker on 4-core node | ~360,000 orders/sec |
| Projected throughput per worker per minute | ~21 million orders/min |
| Projected 2-3 node capacity | ~1 million orders/sec |
| Concurrent run planning note | Designed to support multiple simultaneous runs; current planning target mentioned in benchmarking notes is 10 at a time |

These numbers useful because they show the platform has room to scale before the load generator becomes the limiting component. Production numbers should be revalidated on the exact EKS node type, kernel, networking mode, Kafka settings, and workload mix used for the event.

## Key Features

- **Untrusted-code sandboxing**: participant algorithms run in isolated Kubernetes pods with hardened security settings.
- **Deterministic workloads**: every participant receives the same logical test stream for a given seed and workload mix.
- **Kernel-side latency measurement**: request and response timing is captured outside the participant's process using Linux eBPF hooks.
- **Correctness validation**: outputs are replayed through a reference model before a score is accepted.
- **Latency histograms**: telemetry is aggregated into HDR histograms for percentile-based analysis.
- **Live leaderboard**: APIs and frontend expose scores, run details, and live updates.
- **Cloud-ready deployment**: Terraform and Kubernetes manifests support AWS EKS deployment.
- **Local development stack**: Docker Compose brings up the core data, Kafka, platform, and observability dependencies.

## How It Works

For each submission, IICPC follows this flow:

1. The participant uploads an algorithm.
2. The build system creates a runnable container image.
3. The sandbox orchestrator starts the algorithm in an isolated pod.
4. The bot fleet sends deterministic market traffic to the algorithm.
5. The latency service records when requests enter and responses leave the pod.
6. Telemetry services aggregate latency and throughput metrics.
7. The correctness validator checks whether the algorithm behaved correctly.
8. The score computer produces the final leaderboard score.

The important detail is where measurement happens. IICPC does not ask the submitted algorithm how fast it was. It observes network traffic from outside the algorithm and computes latency from those observations.

## Architecture

Architecture diagrams are kept as Mermaid files under [`mermaids/`](./mermaids/) so they can be rendered in GitHub, documentation sites, or architecture review decks without duplicating diagrams in the README.

Available diagrams:

- [`mermaids/end_to_end_pipeline.mermaid`](./mermaids/end_to_end_pipeline.mermaid): sequence-level view of one benchmark run, from user trigger to final score.
- [`mermaids/k8s_namespaces.mermaid`](./mermaids/k8s_namespaces.mermaid): deployment-level view of the Kubernetes namespaces and the services in each tier.

The README intentionally does not inline the full architecture diagram. The Mermaid files are the source of truth for architecture visuals, while [design.md](./design.md) is the source of truth for detailed engineering rationale.

## Tech Stack

| Area | Technology |
|---|---|
| APIs and orchestration | Go |
| Load generation and telemetry | Rust |
| Frontend | Next.js |
| Event bus | Kafka |
| Metadata storage | PostgreSQL |
| Time-series metrics | TimescaleDB |
| Hot snapshots | Redis |
| Artifact storage | MinIO / S3 |
| Runtime isolation | Kubernetes |
| Kernel measurement | eBPF, XDP, tc |
| Observability | Prometheus, Grafana, Loki |
| Cloud infrastructure | Terraform, AWS EKS, ECR, IRSA, KEDA |

## Repository Layout

```text
frontend/                  Web application
services/                  Backend, benchmark, telemetry, and scoring services
libs/                      Shared Go and Rust libraries
schemas/                   Shared event and Kafka topic schemas
k8s/                       Kubernetes manifests
infra/                     AWS Terraform and deployment Makefile
ops/                       Prometheus, Grafana, Kafka, and operational config
bootstrap/                 Secret templates and bootstrap notes
docs/                      Local run and cloud deployment guides
design.md                  Detailed engineering design
testing_commands.md        Test command reference
```

## Getting Started

Use the local run guide for setup commands:

- [Local development guide](./docs/local-run.md)

The local setup uses Docker Compose for dependencies such as PostgreSQL, TimescaleDB, Redis, Kafka, MinIO, Prometheus, Grafana, and Loki. Individual services can then be run from source.

## Testing

Test commands are maintained separately:

- [Testing command reference](./docs/testing_commands.md)

The test guide covers Go tests, Rust tests, frontend checks, integration tests, and Kubernetes smoke checks.

## Deployment

AWS deployment instructions are in:

- [AWS deployment guide](./docs/aws-deployment.md)
- [Infrastructure README](./infra/README.md)
- [Secret bootstrap guide](./bootstrap/README.md)

The AWS path uses EKS, ECR, Terraform, Kubernetes manifests, IRSA, KEDA, and the AWS Load Balancer Controller.

## Architecture Document

TODO: Add link to the final architecture document.

Suggested target:

- `docs/architecture.pdf` or an externally hosted architecture review document

## Demo Video

TODO: Add link to the project demo video.

Suggested target:

- Product walkthrough
- Benchmark run demo
- Leaderboard and telemetry demo

## Design Notes

The detailed system design is documented in [design.md](./design.md). It explains the measurement model, deterministic workload generation, sandboxing strategy, telemetry pipeline, correctness validation, and scoring model.

## Development Team

- [@agrawalx](https://github.com/agrawalx)
- [@akronim26](https://github.com/akronim26)
