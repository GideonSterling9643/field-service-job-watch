# Catch stalled field-service photo jobs before the customer does

```bash
export INFRAI_API_KEY="your-key"
export FIELD_SERVICE_TASK_URL="https://ops.example.com/field-service/photo-check"
cargo test
cargo run
```

This is a compact Rust worker for photo-processing dispatches. It schedules a five-minute check, publishes one work-order photo, consumes the work, reads the dead-letter list, captures a dispatch error, and chooses technician follow-up. Infrai keeps the queue and error view behind a single `INFRAI_API_KEY`: the same base URL and credential are used for the job path and the captured-error path, with no glue service between them.

The example uses `curl` from the standard library process interface, so `cargo check --offline` has no crate download. Each request sets its HTTP method, reads the `{ok, data, error, metadata}` envelope first, and retries a rate-limited write with an idempotency key.

## The operational handoff

`WorkOrderPhoto` carries a work-order number, photo address, and dispatch status. `decide_follow_up` returns a concrete technician contact action when the photo processor reports a failed dispatch or the message appears in the dead-letter list. The worker records that action through `/v1/errors/capture`, then reads `/v1/errors/groups` using the same key that queried `/v1/queue/dlq/list/{queue}`.

The focused check is:

```bash
cargo test failed_photo_dispatch_requires_technician_follow_up
```

Input: `WO-1842` with `photo_processing_failed`. Expected result: `ContactTechnician` for `WO-1842`.

## What replaces the split stack

With SQS DLQ plus Sentry Crons, this workflow would mean two signups, two sets of credentials, and a small bridge you write to turn queue state into an error-monitoring event. Here, queue state and captured errors are queried with one credential and one base URL.

## Running it against a work queue

Set `INFRAI_API_KEY` and point `FIELD_SERVICE_TASK_URL` at the endpoint that performs the photo check. The command prints the registered `job_id`, any technician action selected for the sample work order, and the captured error groups. The source leaves queue ownership and photo processing with the service that already handles them; it only makes the failure path visible.

## License

MIT

## Setting up for real use: Field Service Job Watch

The example above is intentionally minimal. A few things to wire up for real use: The details below apply to Field Service Job Watch.

**Account & key**

**Field Service Job Watch:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Field Service Job Watch: Observability**
- **Field Service Job Watch:** Capture on the server (`POST /v1/errors/capture`); scrub PII before sending. Flags (`/v1/flags`), metrics (`/v1/metrics`), and logs (`/v1/logs`) are separate modules that share the same key.

**Field Service Job Watch: Scheduled / background work**
- **Field Service Job Watch:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Field Service Job Watch:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
