# GhostMQ Roadmap

This document outlines the planned direction for GhostMQ as a fast, high-throughput, in-memory messaging queue.

## Current Focus

*   **V1 Core Implementation**: Deliver a lightweight in-memory queue engine with configurable backpressure, at-least-once delivery semantics, an HTTP API, and YAML-based configuration.
*   **Performance First**: Optimize for low-latency enqueue/dequeue operations and efficient memory usage.
*   **Operational Simplicity**: Keep deployment simple with a single-node containerized runtime.

## Near-Term Goals

*   Improve API ergonomics and observability
*   Add more queue-level metrics and health reporting
*   Strengthen reliability behavior around retries and acknowledgements
*   Expand examples and integration documentation
*   Evaluate lower-latency transport options, including gRPC, to reduce request overhead

## Future Plans

*   Introduce gRPC-based transport for higher-throughput client/server communication
*   Provide official SDKs for Node.js, Python, Java, and Go
*   Clustering and distributed queue topologies
*   Advanced monitoring and metrics
*   Authentication and Authorization
*   Web UI for management
