import json, time, uuid, gzip, io, boto3, os
from datetime import datetime, timezone
from typing import Dict, Any, List
from .config import S3_BUCKET, S3_PREFIX, S3_MAX_RECORDS, S3_ROLL_SECONDS, S3_GZIP

class S3Sink:
    def __init__(self):
        self.enabled = bool(S3_BUCKET)
        self.s3 = boto3.client("s3") if self.enabled else None
        self.buf: List[str] = []
        self.created_at = time.time()

    def _now_parts(self):
        now = datetime.now(timezone.utc)
        return {
            "date": now.strftime("%Y-%m-%d"),
            "hour": now.strftime("%H"),
            "ts": now.strftime("%Y%m%dT%H%M%SZ")
        }

    def _make_key(self) -> str:
        parts = self._now_parts()
        uid = uuid.uuid4().hex[:8]
        base = f"{S3_PREFIX.rstrip('/')}/date={parts['date']}/hour={parts['hour']}"
        fname = f"batch_{parts['ts']}_{uid}.ndjson"
        if S3_GZIP:
            fname += ".gz"
        return f"{base}/{fname}"

    def write_record(self, rec: Dict[str, Any]):
        if not self.enabled:
            return
        self.buf.append(json.dumps(rec, separators=(",", ":"), ensure_ascii=False))

    def should_flush(self) -> bool:
        if not self.enabled:
            return False
        if len(self.buf) >= S3_MAX_RECORDS:
            return True
        if (time.time() - self.created_at) >= S3_ROLL_SECONDS and self.buf:
            return True
        return False

    def flush(self):
        if not self.enabled or not self.buf:
            return
        body_bytes: bytes
        joined = "\n".join(self.buf).encode("utf-8")
        if S3_GZIP:
            bio = io.BytesIO()
            with gzip.GzipFile(fileobj=bio, mode="wb") as gzf:
                gzf.write(joined)
            body_bytes = bio.getvalue()
            content_type = "application/x-ndjson"
            content_encoding = "gzip"
        else:
            body_bytes = joined
            content_type = "application/x-ndjson"
            content_encoding = None

        key = self._make_key()
        extra = {"ContentType": content_type}
        if content_encoding:
            extra["ContentEncoding"] = content_encoding

        self.s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body_bytes, **extra)
        # reset buffer
        self.buf.clear()
        self.created_at = time.time()

    def close(self):
        # final flush on shutdown
        self.flush()
