# llmq - low latency message queue

A multiprocess message queue designed to work without any work required in
the kernel

## high level architecture

Client libraries use llmq in a publisher/subscriber manner.

When a pubsub is initialized, it connects to the broker over a control socket.
The broker establishes rx and tx ringbuffers over shared memory, hereafter
called the dataplane.

The broker itself runs as a seperate process that constantly reads messages
from the client publishers, and appropriately copies them to client subscribers

## project structure

- `/src/adapter`

  Uses the frame decoder/encoder pattern to allow creating Sinks and Streams of
  arbitrary types on top of async Unix sockets

- `/src/broker`

  Handles control plane requests from clients, as well as runs data plane
  hotloop

- `/src/protocol`

  Actual types that are encoded and decoded using the adapter, so that clients
  and broker can communicate over the control plane.

- `/src/queue`

  Fast ringbuffer implementation on top of Unix shared memory. Allows for
  multiprocess queues with no locks and no kernel synchronization primitives.

- `pubsub`

  Actual library implementation that most clients should use
