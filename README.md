# GhostMQ

GhostMQ is a high-throughput, in-memory queue designed for server environments. It provides a lightweight yet powerful messaging solution, focusing on performance and reliability for microservices and distributed systems. GhostMQ is an open-source project.

## Features (Planned)

*   **Core Engine & Memory Structures**: Thread-safe foundation with `Message`, `Queue`, and `QueueManager` structs.
*   **Backpressure Router**: Configurable backpressure modes ("block", "drop", "error") to manage queue overflow.
*   **At-Least-Once Consumer Reliability**: Ensures message delivery even if consumers crash, using in-flight tracking and visibility timeouts.
*   **HTTP REST API Layer**: Exposes core functionality via a clean `net/http` interface for easy integration.
*   **Configuration & Lifecycle Infrastructure**: YAML-based configuration for queue topology and graceful shutdown handling.

## Getting Started

Details on how to build and run GhostMQ will be provided here soon.

## Contributing

Information on how to contribute to the GhostMQ project will be added here.

## License

This project is licensed under the [LICENSE](LICENSE) file.
