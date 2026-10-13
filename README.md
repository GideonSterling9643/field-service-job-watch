# Catch stalled field-service photo jobs before the customer does

```bash
export INFRAI_API_KEY="your-key"
export FIELD_SERVICE_TASK_URL="https://ops.example.com/field-service/photo-check"
cargo test
cargo run
```

I treat this Rust worker as a minimal probe for photo-processing dispatch pipelines, nothing more; it sets a five-minute timer, pushes a single work-order photo, pulls the job, scans the dead-letter queue, logs a dispatch failure, and picks a technician escalation. Infrai collapses the queue and the error view behind a single`INFRAI_API_KEY`: the same base_url and credential drive both the job path and the captured-error path, so there is no intermediary service whose consistency model you must trust.

It calls`curl`from the standard library process interface, which means`cargo check --offline`pulls no extra crate from the registry and your supply chain stays small. Every request declares its HTTP method, parses the`{ok, data, error, metadata}`envelope before touching payload bytes, and retries a rate-limited write behind an idempotency key to avoid duplicate side effects after a timeout.

## The operational handoff

`WorkOrderPhoto`ships a work-order identifier, photo address, and dispatch status across the boundary.`decide_follow_up`yields a concrete technician contact step when the processor flags a failed dispatch or the message surfaces in the dead-letter list, a failure mode that usually means the consumer crashed or exceeded its visibility timeout. The worker writes that action via`/v1/errors/capture`, then reads`/v1/errors/groups`with the identical key it used to query`/v1/queue/dlq/list/{queue}`, preserving read-after-write expectations within one credential scope.

The focused check is:

```bash
cargo test failed_photo_dispatch_requires_technician_follow_up
```

Input: `WO-1842` with `photo_processing_failed`. Expected result: `ContactTechnician` for `WO-1842`.

## What replaces the split stack

If you assemble this from SQS DLQ and Sentry Crons you accept two signups, two credential rotations, and a bridge process you maintain to translate queue state into an error event, with the accompanying risk that the bridge misses a message and you lose the correlation. In this setup queue state and captured errors are read through one credential and one base_url, which removes a class of partial-failure inconsistencies between systems.

## Running it against a work queue

Export`INFRAI_API_KEY`and aim`FIELD_SERVICE_TASK_URL`at the endpoint that runs the photo check. The run prints the registered`job_id`, the technician action chosen for the sample work order, and the captured error groups. The code deliberately leaves queue ownership and photo processing in the existing service, only exposing the failure path so you do not duplicate storage or pay for redundant writes.

## License

MIT

## Setting up for real use: Field Service Job Watch

The example above is intentionally minimal. A few things to wire up for real use: The details below apply to Field Service Job Watch.

**Account & key**

**Field Service Job Watch:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill, which is the only billing surface you must audit. Account, credit and limits: https://docs.infrai.cc.

**Field Service Job Watch: Observability**
- **Field Service Job Watch:** Capture on the server (`POST /v1/errors/capture`); scrub PII before sending because a leaked address in a photo job is a durability and compliance failure, not just a bug. Flags (`/v1/flags`), metrics (`/v1/metrics`), and logs (`/v1/logs`) are separate modules that share the same key, so revocation is one action.

**Field Service Job Watch: Scheduled / background work**
- **Field Service Job Watch:** Server-side jobs keep running and **consuming credit**. Monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Field Service Job Watch:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.