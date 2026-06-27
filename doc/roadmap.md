# GhostMQ Roadmap

This document outlines the planned direction for GhostMQ as a fast, high-throughput, in-memory messaging queue.

## Current Focus

*   **V1 Core Implementation**: Deliver a lightweight in-memory queue engine with configurable backpressure, at-least-once delivery semantics, an HTTP API, and YAML-based configuration.
*   **Performance First**: Optimize for low-latency enqueue/dequeue operations and efficient memory usage.
*   **Operational Simplicity**: Keep deployment simple with a single-node containerized runtime.
*   **Observability**: Expose lightweight health and queue metrics for operational visibility.
*   **API Hardening**: Improve request validation, error semantics, and developer ergonomics.
*   **Documentation**: Provide architecture guidance, integration examples, and clear onboarding paths.

## Near-Term Goals

*   Add richer queue-level health reporting and operational metrics
*   Strengthen reliability behavior around retries and acknowledgements
*   Evaluate lower-latency transport options, including gRPC, to reduce request overhead
*   Add starter SDK examples for Node.js, Python, Java, and Go

## Future Plans

*   Introduce gRPC-based transport for higher-throughput client/server communication
*   Provide official SDKs for Node.js, Python, Java, and Go
*   Clustering and distributed queue topologies
*   Advanced monitoring and metrics
*   Authentication and Authorization
*   Web UI for management
