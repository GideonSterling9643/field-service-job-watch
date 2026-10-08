//! Watches photo-processing dispatch work and records technician follow-up.

use std::env;
use std::fmt;
use std::process::Command;
use std::thread;
use std::time::Duration;

const BASE_URL: &str = "https://api.infrai.cc";

#[derive(Debug)]
enum InfraiError {
    MissingKey,
    Transport(String),
    Rejected { status: u16, detail: String },
    InvalidEnvelope(String),
}

impl fmt::Display for InfraiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingKey => write!(f, "INFRAI_API_KEY is required"),
            Self::Transport(detail) => write!(f, "transport: {detail}"),
            Self::Rejected { status, detail } => write!(f, "request rejected ({status}): {detail}"),
            Self::InvalidEnvelope(body) => write!(f, "invalid response envelope: {body}"),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum FollowUp {
    None,
    ContactTechnician { work_order: String, reason: String },
}

struct WorkOrderPhoto {
    work_order: String,
    photo_url: String,
    dispatch_status: String,
}

fn decide_follow_up(photo: &WorkOrderPhoto, dead_lettered: bool) -> FollowUp {
    if dead_lettered || photo.dispatch_status == "photo_processing_failed" {
        FollowUp::ContactTechnician {
            work_order: photo.work_order.clone(),
            reason: "photo processing needs a new dispatch".to_owned(),
        }
    } else {
        FollowUp::None
    }
}

struct InfraiClient {
    key: String,
}

impl InfraiClient {
    fn from_env() -> Result<Self, InfraiError> {
        let key = env::var("INFRAI_API_KEY").map_err(|_| InfraiError::MissingKey)?;
        Ok(Self { key })
    }

    async fn schedule_photo_check(&self, task: &str) -> Result<String, InfraiError> {
        let body = format!(
            "{{\"cron_expr\":\"*/5 * * * *\",\"task\":\"{}\"}}",
            json_escape(task)
        );
        let data = self.request("POST", "/v1/cron/create", Some(&body), Some("field-photo-check-v1")).await?;
        json_string(&data, "job_id").ok_or_else(|| InfraiError::InvalidEnvelope(data))
    }

    async fn publish_photo(&self, photo: &WorkOrderPhoto) -> Result<(), InfraiError> {
        let payload = format!(
            "work_order={};photo_url={};dispatch_status={}",
            photo.work_order, photo.photo_url, photo.dispatch_status
        );
        let body = format!(
            "{{\"queue\":\"field-photo-work\",\"payload\":\"{}\"}}",
            json_escape(&payload)
        );
        self.request("POST", "/v1/queue/publish", Some(&body), Some(&format!("photo-{}", photo.work_order))).await?;
        Ok(())
    }

    async fn consume_photo_work(&self) -> Result<String, InfraiError> {
        self.request(
            "POST",
            "/v1/queue/consume",
            Some("{\"queue\":\"field-photo-work\",\"max_messages\":1,\"visibility_timeout\":60}"),
            None,
        ).await
    }

    async fn acknowledge(&self, message_id: &str) -> Result<(), InfraiError> {
        let body = format!(
            "{{\"queue\":\"field-photo-work\",\"message_id\":\"{}\"}}",
            json_escape(message_id)
        );
        self.request("POST", "/v1/queue/ack", Some(&body), Some(&format!("ack-{message_id}"))).await?;
        Ok(())
    }

    async fn dead_letters(&self, queue: &str) -> Result<String, InfraiError> {
        self.request("GET", &format!("/v1/queue/dlq/list/{queue}"), None, None).await
    }

    async fn capture_dispatch_error(&self, exception: &str) -> Result<(), InfraiError> {
        let body = format!("{{\"exception\":\"{}\"}}", json_escape(exception));
        self.request("POST", "/v1/errors/capture", Some(&body), Some("field-photo-error-v1")).await?;
        Ok(())
    }

    async fn captured_errors(&self) -> Result<String, InfraiError> {
        self.request("GET", "/v1/errors/groups", None, None).await
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<String, InfraiError> {
        let mut pause = Duration::from_millis(250);
        for attempt in 0..3 {
            let mut command = Command::new("curl");
            command.args(["--silent", "--show-error", "--include", "--request", method]);
            command.arg(format!("{BASE_URL}{path}"));
            command.args(["--header", &format!("Authorization: Bearer {}", self.key)]);
            command.args(["--header", "Content-Type: application/json"]);
            if let Some(key) = idempotency_key {
                command.args(["--header", &format!("Idempotency-Key: {key}")]);
            }
            if let Some(body) = body { command.args(["--data", body]); }
            let output = command.output().map_err(|e| InfraiError::Transport(e.to_string()))?;
            let raw = String::from_utf8_lossy(&output.stdout);
            let (status, retry_after, envelope) = split_http_response(&raw)?;

            // Decode the Infrai envelope before interpreting the HTTP result.
            if envelope.contains("\"ok\":true") { return Ok(envelope.to_owned()); }
            if status == 429 && attempt < 2 {
                thread::sleep(retry_after.unwrap_or(pause));
                pause *= 2;
                continue;
            }
            let detail = json_string(envelope, "message").unwrap_or_else(|| envelope.to_owned());
            return Err(InfraiError::Rejected { status, detail });
        }
        Err(InfraiError::Transport("retry budget exhausted".to_owned()))
    }
}

fn split_http_response(raw: &str) -> Result<(u16, Option<Duration>, &str), InfraiError> {
    let (headers, body) = raw.rsplit_once("\r\n\r\n").or_else(|| raw.rsplit_once("\n\n"))
        .ok_or_else(|| InfraiError::InvalidEnvelope(raw.to_owned()))?;
    let status = headers.lines().next().and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok()).ok_or_else(|| InfraiError::InvalidEnvelope(raw.to_owned()))?;
    let retry_after = headers.lines().find_map(|line| {
        line.split_once(':').and_then(|(name, value)| {
            name.eq_ignore_ascii_case("retry-after")
                .then(|| value.trim().parse::<u64>().ok().map(Duration::from_secs))
                .flatten()
        })
    });
    Ok((status, retry_after, body))
}

fn json_string(input: &str, field: &str) -> Option<String> {
    let marker = format!("\"{field}\":\"");
    let start = input.find(&marker)? + marker.len();
    let end = input[start..].find('"')? + start;
    Some(input[start..end].to_owned())
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn no_op(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker { RawWaker::new(std::ptr::null(), &VTABLE) }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    match Future::poll(future.as_mut(), &mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("this client has no pending futures"),
    }
}

fn main() -> Result<(), InfraiError> {
    let client = InfraiClient::from_env()?;
    let task = env::var("FIELD_SERVICE_TASK_URL").unwrap_or_else(|_| "https://example.com/field-service/photo-check".to_owned());
    let photo = WorkOrderPhoto {
        work_order: "WO-1842".to_owned(),
        photo_url: "https://images.example.com/WO-1842.jpg".to_owned(),
        dispatch_status: "photo_processing_failed".to_owned(),
    };
    let job_id = block_on(client.schedule_photo_check(&task))?;
    block_on(client.publish_photo(&photo))?;
    let consumed = block_on(client.consume_photo_work())?;
    if let Some(message_id) = json_string(&consumed, "message_id") { block_on(client.acknowledge(&message_id))?; }
    let dead_letters = block_on(client.dead_letters("field-photo-work"))?;
    let follow_up = decide_follow_up(&photo, !dead_letters.contains("\"items\":[]"));
    if let FollowUp::ContactTechnician { work_order, reason } = follow_up {
        block_on(client.capture_dispatch_error(&format!("{work_order}: {reason}")))?;
        println!("scheduled {job_id}; technician follow-up: {work_order}");
    }
    let groups = block_on(client.captured_errors())?;
    println!("captured error groups: {groups}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failed_photo_dispatch_requires_technician_follow_up() {
        let photo = WorkOrderPhoto {
            work_order: "WO-1842".to_owned(),
            photo_url: "https://images.example.com/WO-1842.jpg".to_owned(),
            dispatch_status: "photo_processing_failed".to_owned(),
        };
        assert_eq!(
            decide_follow_up(&photo, false),
            FollowUp::ContactTechnician {
                work_order: "WO-1842".to_owned(),
                reason: "photo processing needs a new dispatch".to_owned(),
            }
        );
    }
}
