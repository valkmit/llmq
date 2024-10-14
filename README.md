# llmq - low latency message queue

A multiprocess message queue designed to work without any work required in
the kernel

## high level architecture

Client libraries use llmq in a publisher/subscriber manner.

When a pubsub is initialized, it connects to the broker over a control socket.
The broker establishes rx and tx ringbuffers over shared memory, hereafter
called the dataplane.
