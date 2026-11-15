import json
import uuid

from .analysis.engine import analysis_plan
from .analysis.windows import normalize_request
from .errors import LedgerError, conflict, missing, require
from .values import canonical, fingerprint, identifier, integer, page_size, text, utc_after, utc_now

STATES = {"queued", "running", "succeeded", "failed", "cancelled"}
ACTIVE = {"queued", "running"}
RETRYABLE = {"failed", "cancelled"}


class LeaseLost(Exception):
    """持有租约期间被更新的执行者取代，或作业已被取消：一切写入必须放弃。"""


class Jobs:
    """本地分析作业：提交即固定规范化请求与数据版本，按统计窗口断点续跑。

    - 进度只来自已提交的检查点（analysis_job_steps），对应真实完成的统计范围；
    - 多个工作进程通过租约竞争领取，epoch 为栅栏令牌，迟到写入一律被拒绝；
    - 完成时在同一写事务中登记报告版本（editions）并置成功，中间数据永不外流。
    """

    def __init__(self, database, engine, editions):
        self.database = database
        self.engine = engine
        self.editions = editions

    def submit(self, request_key, request, actor="operator"):
        identifier(request_key, "request_key")
        actor = text(actor, "操作人", 100)
        normalized = normalize_request(request)
        digest = fingerprint(normalized)
        with self.database.transaction(write=True) as connection:
            revision = self.database.snapshot_revision(connection, normalized.get("revision"))
            pinned = {**normalized, "revision": revision}
            existing = self._by_key(connection, request_key)
            if existing is not None:
                if existing["request_digest"] != digest:
                    raise conflict("分析请求号已用于不同请求")
                return self.decode(existing, connection)
            edition = connection.execute(
                "SELECT * FROM editions WHERE request_key=?", (request_key,)).fetchone()
            now = utc_now()
            if edition is not None:
                if edition["request_digest"] != digest:
                    raise conflict("分析请求号已用于不同请求")
                require(edition["data_revision"] == revision, "request_key_conflict",
                        "分析请求号已被其他数据版本的报告占用，请更换请求号", 409)
                identity = self._insert(connection, request_key, digest, pinned, "succeeded", now)
                connection.execute(
                    "UPDATE analysis_jobs SET edition_id=?, result_digest=?, started_at=?, completed_at=? WHERE id=?",
                    (edition["id"], edition["result_digest"], now, now, identity))
                self.record(connection, identity, 0, "adopted", actor,
                            {"edition_id": edition["id"], "data_revision": revision})
                return self.get(identity, connection)
            identity = self._insert(connection, request_key, digest, pinned, "queued", now)
            self.record(connection, identity, 0, "submitted", actor, {"data_revision": revision})
            return self.get(identity, connection)

    def _insert(self, connection, request_key, digest, pinned, state, now):
        identity = "job-" + uuid.uuid4().hex
        plan = analysis_plan(pinned)
        plan_digest = fingerprint({"format": 1, "steps": plan})
        connection.execute(
            "INSERT INTO analysis_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (identity, request_key, digest, pinned["name"], pinned["revision"], canonical(pinned),
             canonical(plan), plan_digest, state, 0, 0, None, None, None, None, None, now, now, None, None))
        return identity

    def get(self, identity, connection=None):
        identifier(identity)
        if connection is None:
            with self.database.transaction() as opened:
                return self.get(identity, opened)
        return self.decode(self._row(connection, identity), connection)

    def list(self, state=None, limit=100, offset=0):
        page_size(limit, offset)
        parameters = []
        clause = ""
        if state is not None:
            require(state in STATES, "invalid_state", "作业状态无效")
            clause = " WHERE state=?"
            parameters.append(state)
        with self.database.transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM analysis_jobs" + clause + " ORDER BY created_at,id LIMIT ? OFFSET ?",
                parameters + [limit, offset]).fetchall()
            return [self.decode(row, connection) for row in rows]

    def events(self, identity, limit=100, offset=0):
        identifier(identity)
        page_size(limit, offset)
        with self.database.transaction() as connection:
            self._row(connection, identity)
            rows = connection.execute(
                "SELECT * FROM analysis_job_events WHERE job_id=? ORDER BY id LIMIT ? OFFSET ?",
                (identity, limit, offset)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def cancel(self, identity, actor="operator"):
        identifier(identity)
        actor = text(actor, "操作人", 100)
        with self.database.transaction(write=True) as connection:
            row = self._row(connection, identity)
            if row["state"] == "cancelled":
                return self.decode(row, connection)
            require(row["state"] in ACTIVE, "invalid_transition",
                    "仅排队或运行中的作业可以取消", 409, {"state": row["state"]})
            now = utc_now()
            updated = connection.execute(
                "UPDATE analysis_jobs SET state='cancelled', owner=NULL, lease_expires_at=NULL,"
                " completed_at=?, updated_at=? WHERE id=? AND state IN ('queued','running')",
                (now, now, identity)).rowcount
            require(updated == 1, "conflict", "作业状态已变化，请重新读取", 409)
            self.record(connection, identity, row["epoch"], "cancelled", actor,
                        {"from_state": row["state"]})
            return self.get(identity, connection)

    def retry(self, identity, actor="operator"):
        identifier(identity)
        actor = text(actor, "操作人", 100)
        with self.database.transaction(write=True) as connection:
            row = self._row(connection, identity)
            require(row["state"] in RETRYABLE, "invalid_transition",
                    "仅失败或已取消的作业可以重试", 409, {"state": row["state"]})
            now = utc_now()
            updated = connection.execute(
                "UPDATE analysis_jobs SET state='queued', owner=NULL, lease_expires_at=NULL, error=NULL,"
                " completed_at=NULL, updated_at=? WHERE id=? AND state IN ('failed','cancelled')",
                (now, identity)).rowcount
            require(updated == 1, "conflict", "作业状态已变化，请重新读取", 409)
            self.record(connection, identity, row["epoch"], "retried", actor,
                        {"from_state": row["state"], "kept_checkpoints": self._checkpoint_count(connection, identity)})
            return self.get(identity, connection)

    def claim(self, owner, lease_seconds=30):
        identifier(owner, "owner")
        integer(lease_seconds, "lease_seconds", 1, 3600)
        with self.database.transaction(write=True) as connection:
            now = utc_now()
            row = connection.execute(
                "SELECT * FROM analysis_jobs WHERE state='queued'"
                " OR (state='running' AND lease_expires_at<?) ORDER BY created_at,id LIMIT 1",
                (now,)).fetchone()
            if row is None:
                return None
            expires = utc_after(lease_seconds)
            updated = connection.execute(
                "UPDATE analysis_jobs SET state='running', owner=?, epoch=epoch+1, attempts=attempts+1,"
                " lease_expires_at=?, started_at=?, updated_at=? WHERE id=? AND epoch=?",
                (owner, expires, now, now, row["id"], row["epoch"])).rowcount
            if updated != 1:
                return None
            self.record(connection, row["id"], row["epoch"] + 1, "claimed", owner,
                        {"attempt": row["attempts"] + 1, "lease_expires_at": expires,
                         "reclaimed": row["state"] == "running"})
            return self.get(row["id"], connection)

    def run_claimed(self, job, owner, lease_seconds=30, max_steps=None):
        identifier(owner, "owner")
        require(job["state"] == "running" and job["owner"] == owner,
                "not_claimed", "作业未由该执行者持有", 409)
        epoch = job["epoch"]
        try:
            context, pending = self._resume(job)
            ran = 0
            for step in pending:
                if max_steps is not None and ran >= max_steps:
                    break
                payload = self._compute(job, step, context)
                self._apply(context, step, payload)
                self._checkpoint(job, epoch, owner, step, payload, lease_seconds)
                ran += 1
            if ran == len(pending):
                self._complete(job, epoch, owner, context)
        except LeaseLost:
            pass
        except LedgerError as exc:
            if exc.code == "busy":
                self._requeue(job, epoch, owner)
            else:
                self._fail(job, epoch, owner, exc.as_dict())
        except Exception as exc:  # noqa: BLE001 - 任何意外都必须落成可重试的失败，而不是静默丢作业
            self._fail(job, epoch, owner,
                       {"code": "internal_error", "message": f"作业执行异常：{exc}", "details": {}})
        return self.get(job["id"])

    def work(self, owner, lease_seconds=30, limit=None):
        processed = 0
        while limit is None or processed < limit:
            job = self.claim(owner, lease_seconds)
            if job is None:
                break
            self.run_claimed(job, owner, lease_seconds)
            processed += 1
        return processed

    def require_no_active(self, request_key, connection):
        row = connection.execute(
            "SELECT id, state FROM analysis_jobs WHERE request_key=? AND state IN ('queued','running')",
            (request_key,)).fetchone()
        if row is not None:
            raise LedgerError("job_in_progress", "该请求号的分析作业进行中，请查询作业进度或等待完成",
                              409, {"job_id": row["id"], "state": row["state"]})

    def _resume(self, job):
        with self.database.transaction() as connection:
            rows = connection.execute(
                "SELECT step, payload FROM analysis_job_steps WHERE job_id=? AND plan_digest=? ORDER BY rowid",
                (job["id"], job["plan_digest"])).fetchall()
        done = {row["step"]: json.loads(row["payload"]) for row in rows}
        context = {"references": None, "periods": {}, "lineage": [], "comparison": None}
        pending = []
        for step in job["plan"]:
            key = step["key"]
            if key == "result":
                continue
            if key in done:
                self._apply(context, step, done[key])
            else:
                pending.append(step)
        return context, pending

    def _apply(self, context, step, payload):
        key = step["key"]
        if key == "references":
            context["references"] = payload
        elif key.startswith("period:"):
            name = key.split(":", 1)[1]
            context["periods"][name] = payload["period"]
            context["lineage"].extend(payload["lineage"])
        elif key == "comparison":
            context["comparison"] = payload
        else:
            raise LedgerError("unknown_step", f"未知分析步骤：{key}", 500)

    def _compute(self, job, step, context):
        with self.database.transaction() as connection:
            return self.engine.execute_step(step["key"], job["request"], job["data_revision"], context, connection)

    def _checkpoint(self, job, epoch, owner, step, payload, lease_seconds):
        with self.database.transaction(write=True) as connection:
            now = utc_now()
            updated = connection.execute(
                "UPDATE analysis_jobs SET lease_expires_at=?, updated_at=?"
                " WHERE id=? AND epoch=? AND owner=? AND state='running'",
                (utc_after(lease_seconds), now, job["id"], epoch, owner)).rowcount
            if updated != 1:
                raise LeaseLost(job["id"])
            connection.execute(
                "INSERT INTO analysis_job_steps VALUES (?,?,?,?,?,?,?)",
                (job["id"], step["key"], epoch, job["plan_digest"], canonical(step["scope"]),
                 canonical(payload), now))
            self.record(connection, job["id"], epoch, "checkpoint", owner, {"step": step["key"]})

    def _complete(self, job, epoch, owner, context):
        result = self.engine.assemble(job["request"], job["data_revision"], context["references"],
                                      context["periods"], context["lineage"], context["comparison"])
        result_digest = fingerprint(result)
        with self.database.transaction(write=True) as connection:
            now = utc_now()
            edition = connection.execute(
                "SELECT * FROM editions WHERE request_key=?", (job["request_key"],)).fetchone()
            adopted = False
            if edition is not None:
                require(edition["request_digest"] == job["request_digest"]
                        and edition["data_revision"] == job["data_revision"],
                        "request_key_conflict", "请求号已被其他报告占用，作业结果无法登记", 409)
                edition_id = edition["id"]
                adopted = True
            else:
                edition_id = "ed-" + uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO editions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (edition_id, job["request_key"], job["request_digest"], job["request"]["name"],
                     job["data_revision"], canonical(job["request"]), "draft", 1,
                     canonical(result), result_digest, now))
            updated = connection.execute(
                "UPDATE analysis_jobs SET state='succeeded', edition_id=?, result_digest=?,"
                " completed_at=?, updated_at=? WHERE id=? AND epoch=? AND owner=? AND state='running'",
                (edition_id, result_digest, now, now, job["id"], epoch, owner)).rowcount
            if updated != 1:
                raise LeaseLost(job["id"])
            self.record(connection, job["id"], epoch, "succeeded", owner,
                        {"edition_id": edition_id, "adopted": adopted, "result_digest": result_digest})

    def _fail(self, job, epoch, owner, error):
        with self.database.transaction(write=True) as connection:
            now = utc_now()
            updated = connection.execute(
                "UPDATE analysis_jobs SET state='failed', error=?, completed_at=?, updated_at=?"
                " WHERE id=? AND epoch=? AND state='running'",
                (canonical(error), now, now, job["id"], epoch)).rowcount
            if updated:
                self.record(connection, job["id"], epoch, "failed", owner, error)

    def _requeue(self, job, epoch, owner):
        with self.database.transaction(write=True) as connection:
            updated = connection.execute(
                "UPDATE analysis_jobs SET state='queued', owner=NULL, lease_expires_at=NULL, updated_at=?"
                " WHERE id=? AND epoch=? AND state='running'",
                (utc_now(), job["id"], epoch)).rowcount
            if updated:
                self.record(connection, job["id"], epoch, "requeued", owner, {"reason": "busy"})

    def record(self, connection, job_id, epoch, event, actor, payload):
        connection.execute(
            "INSERT INTO analysis_job_events(job_id,epoch,event,actor,payload,created_at) VALUES (?,?,?,?,?,?)",
            (job_id, epoch, event, actor, canonical(payload), utc_now()))

    def decode(self, row, connection):
        job = dict(row)
        job["request"] = json.loads(job["request"])
        job["plan"] = json.loads(job["plan"])
        job["error"] = json.loads(job["error"]) if job["error"] else None
        rows = connection.execute(
            "SELECT step, scope, epoch, completed_at FROM analysis_job_steps"
            " WHERE job_id=? AND plan_digest=? ORDER BY rowid",
            (job["id"], job["plan_digest"])).fetchall()
        checkpoints = {row["step"]: {"key": row["step"], "scope": json.loads(row["scope"]),
                                     "epoch": row["epoch"], "completed_at": row["completed_at"]}
                       for row in rows}
        completed = []
        for step in job["plan"]:
            key = step["key"]
            if key in checkpoints:
                completed.append(checkpoints[key])
            elif job["state"] == "succeeded":
                completed.append({"key": key, "scope": step["scope"], "epoch": job["epoch"],
                                  "completed_at": job["completed_at"]})
        done = {item["key"] for item in completed}
        job["completed_steps"] = completed
        job["pending_steps"] = [step for step in job["plan"] if step["key"] not in done]
        job["progress"] = {"completed": len(done), "total": len(job["plan"])}
        job["current_data_revision"] = self.database.revision(connection)
        job["stale"] = job["data_revision"] != job["current_data_revision"]
        return job

    def _row(self, connection, identity):
        row = connection.execute("SELECT * FROM analysis_jobs WHERE id=?", (identity,)).fetchone()
        if row is None:
            raise missing("分析作业", identity)
        return row

    def _by_key(self, connection, request_key):
        return connection.execute(
            "SELECT * FROM analysis_jobs WHERE request_key=?", (request_key,)).fetchone()

    def _checkpoint_count(self, connection, identity):
        return connection.execute(
            "SELECT COUNT(*) FROM analysis_job_steps WHERE job_id=?", (identity,)).fetchone()[0]
