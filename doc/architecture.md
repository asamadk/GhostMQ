# GhostMQ Architecture

GhostMQ is a lightweight, in-memory messaging queue designed for low-latency communication between services and AI agents.

## Overview

The system is composed of three main layers:

1. Queue Engine
   - Manages in-memory queues and message state
   - Supports configurable backpressure behavior
   - Tracks in-flight messages for at-least-once delivery

2. Service Layer
   - Encapsulates business logic for queue creation, message publish, pop, and ack
   - Validates incoming requests before interacting with the queue engine

3. HTTP API Layer
   - Exposes queue operations over REST
   - Handles request parsing, validation, and response formatting

## Request Flow

1. A client sends a request to the HTTP API.
2. The controller parses the request and delegates to the service layer.
3. The service layer validates the operation and interacts with the queue manager.
4. The queue manager routes the request to the appropriate queue implementation.
5. The queue engine updates its state and returns a result to the caller.

## Core Components

### Queue
A queue stores messages in memory and applies the configured backpressure policy.

### Queue Manager
The queue manager maintains the set of queues and provides operations for creation, lookup, and status reporting.

### Observability
A lightweight metrics recorder tracks queue-level counters for enqueue, dequeue, ack, and reject operations.

## Design Principles

- Keep the runtime simple and single-node
- Optimize for low-latency in-memory operations
- Favor clear separation of concerns between API, service, and queue layers
- Make the system easy to deploy with Docker and simple configuration

## Future Direction

The architecture is expected to evolve toward:

- lower-latency transport options such as gRPC
- SDK-based integrations for Node.js, Python, Java, and Go
- richer operational metrics and health endpoints
