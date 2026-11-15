from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from autumn_ledger.api.server import create_app
from autumn_ledger.application import Ledger
from autumn_ledger.demo import request_data, synthetic_event
from autumn_ledger.errors import LedgerError
from tests.support import LedgerCase

PLAN_KEYS = ["references", "period:current", "period:baseline", "comparison", "result"]


class JobTests(LedgerCase):
    def submit(self, key="job", **changes):
        return self.ledger.jobs.submit(key, request_data(**changes))

    def run_to_success(self, key="job", **changes):
        job = self.submit(key, **changes)
        self.assertEqual(self.ledger.jobs.work("w1"), 1)
        return self.ledger.jobs.get(job["id"])

    def test_submit_pins_normalized_request_and_revision(self):
        job = self.submit()
        self.assertEqual(job["state"], "queued")
        self.assertEqual(job["data_revision"], self.ledger.database.revision())
        self.assertEqual(job["request"]["revision"], job["data_revision"])
        self.assertFalse(job["stale"])
        self.assertEqual([step["key"] for step in job["plan"]], PLAN_KEYS)
        self.assertEqual(job["progress"], {"completed": 0, "total": 5})
        self.assertEqual(job["completed_steps"], [])
        self.assertIsNone(job["edition_id"])
        self.assertEqual(self.ledger.editions.list(), [])

    def test_submit_with_explicit_revision_keeps_it(self):
        revision = self.ledger.database.revision()
        self.ledger.intake.ingest("alpha", "later", [synthetic_event("c1", "2026-11-18")])
        job = self.submit(revision=revision)
        self.assertEqual(job["data_revision"], revision)
        self.assertEqual(self.ledger.jobs.work("w1"), 1)
        done = self.ledger.jobs.get(job["id"])
        edition = self.ledger.editions.get(done["edition_id"])
        self.assertEqual(edition["data_revision"], revision)
        self.assertEqual(edition["result"]["periods"]["current"]["cube"]["totals"]["orders"], 3)

    def test_idempotent_submission_and_conflicting_key(self):
        job = self.submit()
        replay = self.submit()
        self.assertEqual(job["id"], replay["id"])
        self.assertError("conflict", self.submit, start="2026-11-17")
        self.assertEqual(len(self.ledger.jobs.list()), 1)
        done = self.run_to_success()
        again = self.submit()
        self.assertEqual(again["id"], done["id"])
        self.assertEqual(len(self.ledger.jobs.list()), 1)
        self.assertEqual(len(self.ledger.editions.list()), 1)

    def test_worker_result_matches_same_revision_sync_run(self):
        done = self.run_to_success()
        self.assertEqual(done["state"], "succeeded")
        self.assertEqual(done["progress"], {"completed": 5, "total": 5})
        self.assertEqual([step["key"] for step in done["completed_steps"]], PLAN_KEYS)
        edition = self.ledger.editions.get(done["edition_id"])
        self.assertEqual(edition["state"], "draft")
        self.assertEqual(done["result_digest"], edition["result_digest"])
        sync = self.ledger.analysis.run({**request_data(), "revision": done["data_revision"]})
        for key in ("references", "periods", "comparison", "quality", "lineage", "lineage_digest"):
            self.assertEqual(edition["result"][key], sync[key], key)
        self.assertTrue(self.ledger.editions.verify(edition["id"])["ok"])

    def test_new_data_during_run_belongs_to_next_report(self):
        job = self.submit("first")
        pinned = job["data_revision"]
        self.ledger.intake.ingest("alpha", "late-batch", [synthetic_event("c1", "2026-11-18", gross=7000)])
        done = self.run_to_success("first")
        self.assertEqual(done["data_revision"], pinned)
        self.assertTrue(done["stale"])
        edition = self.ledger.editions.get(done["edition_id"])
        self.assertEqual(edition["result"]["periods"]["current"]["cube"]["totals"]["orders"], 3)
        followup = self.submit("second")
        self.assertEqual(followup["data_revision"], pinned + 1)
        followup = self.run_to_success("second")
        updated = self.ledger.editions.get(followup["edition_id"])
        self.assertEqual(updated["result"]["periods"]["current"]["cube"]["totals"]["orders"], 4)

    def test_cancel_queued_job_then_retry_to_completion(self):
        job = self.submit()
        cancelled = self.ledger.jobs.cancel(job["id"], "tester")
        self.assertEqual(cancelled["state"], "cancelled")
        again = self.ledger.jobs.cancel(job["id"], "tester")
        self.assertEqual(again["state"], "cancelled")
        self.assertIsNone(self.ledger.jobs.claim("w1"))
        self.assertEqual(self.ledger.jobs.work("w1"), 0)
        retried = self.ledger.jobs.retry(job["id"], "tester")
        self.assertEqual(retried["state"], "queued")
        done = self.run_to_success()
        self.assertEqual(done["state"], "succeeded")
        names = [event["event"] for event in self.ledger.jobs.events(job["id"])]
        self.assertEqual(names, ["submitted", "cancelled", "retried",
                                 "claimed", "checkpoint", "checkpoint", "checkpoint",
                                 "checkpoint", "succeeded"])

    def test_cancel_running_job_makes_late_worker_back_off(self):
        job = self.submit()
        claimed = self.ledger.jobs.claim("w1")
        self.ledger.jobs.cancel(job["id"], "tester")
        outcome = self.ledger.jobs.run_claimed(claimed, "w1")
        self.assertEqual(outcome["state"], "cancelled")
        self.assertEqual(outcome["completed_steps"], [])
        self.assertEqual(self.ledger.editions.list(), [])
        self.ledger.jobs.retry(job["id"], "tester")
        done = self.run_to_success()
        self.assertEqual(done["state"], "succeeded")
        self.assertEqual(len(self.ledger.editions.list()), 1)

    def test_completion_wins_over_late_cancel(self):
        done = self.run_to_success()
        self.assertError("invalid_transition", self.ledger.jobs.cancel, done["id"], "tester")
        stored = self.ledger.jobs.get(done["id"])
        self.assertEqual(stored["state"], "succeeded")
        self.assertEqual(len(self.ledger.editions.list()), 1)

    def test_retry_requires_failed_or_cancelled_state(self):
        job = self.submit()
        self.assertError("invalid_transition", self.ledger.jobs.retry, job["id"], "tester")
        claimed = self.ledger.jobs.claim("w1")
        self.assertError("invalid_transition", self.ledger.jobs.retry, job["id"], "tester")
        done = self.ledger.jobs.run_claimed(claimed, "w1")
        self.assertEqual(done["state"], "succeeded")
        self.assertError("invalid_transition", self.ledger.jobs.retry, job["id"], "tester")

    def test_run_claimed_requires_holding_the_lease(self):
        job = self.submit()
        self.assertError("not_claimed", self.ledger.jobs.run_claimed, job, "w1")

    def test_stale_lease_holder_cannot_overwrite_new_executor(self):
        job = self.submit()
        claimed = self.ledger.jobs.claim("w1")
        partial = self.ledger.jobs.run_claimed(claimed, "w1", max_steps=1)
        self.assertEqual([step["key"] for step in partial["completed_steps"]], ["references"])
        with self.ledger.database.transaction(write=True) as connection:
            connection.execute("UPDATE analysis_jobs SET lease_expires_at=? WHERE id=?",
                               ("2000-01-01T00:00:00.000000+00:00", job["id"]))
        reclaimed = self.ledger.jobs.claim("w2")
        self.assertEqual(reclaimed["epoch"], 2)
        self.assertEqual(reclaimed["attempts"], 2)
        late = self.ledger.jobs.run_claimed(claimed, "w1")
        self.assertEqual(late["state"], "running")
        self.assertEqual(late["owner"], "w2")
        self.assertEqual([step["key"] for step in late["completed_steps"]], ["references"])
        done = self.ledger.jobs.run_claimed(self.ledger.jobs.get(job["id"]), "w2")
        self.assertEqual(done["state"], "succeeded")
        self.assertEqual(done["completed_steps"][0]["epoch"], 1)
        self.assertEqual(len(self.ledger.editions.list()), 1)
        edition = self.ledger.editions.get(done["edition_id"])
        sync = self.ledger.analysis.run({**request_data(), "revision": done["data_revision"]})
        self.assertEqual(edition["result"]["periods"], sync["periods"])
        self.assertEqual(edition["result"]["lineage_digest"], sync["lineage_digest"])

    def test_failed_job_resumes_from_checkpoints_without_recomputing(self):
        original = self.ledger.analysis.execute_step
        calls = []
        injected = {"done": False}

        def flaky(key, request, revision, context, connection):
            calls.append(key)
            if key == "period:current" and not injected["done"]:
                injected["done"] = True
                raise LedgerError("injected_failure", "注入故障", 500)
            return original(key, request, revision, context, connection)

        self.ledger.analysis.execute_step = flaky
        try:
            job = self.submit()
            self.ledger.jobs.work("w1")
            failed = self.ledger.jobs.get(job["id"])
            self.assertEqual(failed["state"], "failed")
            self.assertEqual(failed["error"]["code"], "injected_failure")
            self.assertEqual([step["key"] for step in failed["completed_steps"]], ["references"])
            retried = self.ledger.jobs.retry(job["id"], "tester")
            self.assertEqual(retried["state"], "queued")
            self.ledger.jobs.work("w2")
        finally:
            self.ledger.analysis.execute_step = original
        done = self.ledger.jobs.get(job["id"])
        self.assertEqual(done["state"], "succeeded")
        self.assertEqual(calls.count("references"), 1)
        self.assertEqual(calls.count("period:current"), 2)
        self.assertEqual(calls.count("period:baseline"), 1)
        self.assertEqual(calls.count("comparison"), 1)
        edition = self.ledger.editions.get(done["edition_id"])
        sync = self.ledger.analysis.run({**request_data(), "revision": done["data_revision"]})
        for key in ("references", "periods", "comparison", "quality", "lineage"):
            self.assertEqual(edition["result"][key], sync[key], key)
        self.assertTrue(self.ledger.editions.verify(edition["id"])["ok"])

    def test_failure_is_deterministic_at_pinned_revision(self):
        job = self.submit(cities=["sz", "nj", "xx"])
        self.ledger.jobs.work("w1")
        failed = self.ledger.jobs.get(job["id"])
        self.assertEqual(failed["state"], "failed")
        self.assertEqual(failed["error"]["code"], "not_found")
        self.ledger.catalog.put("city", "xx", {"name": "合成新城", "region": "合成区域"})
        self.ledger.jobs.retry(job["id"], "tester")
        self.ledger.jobs.work("w1")
        still_failed = self.ledger.jobs.get(job["id"])
        self.assertEqual(still_failed["state"], "failed")
        self.assertEqual(still_failed["error"]["code"], "not_found")
        replacement = self.submit("fixed", cities=["sz", "nj", "xx"])
        self.ledger.jobs.work("w1")
        replacement = self.ledger.jobs.get(replacement["id"])
        self.assertEqual(replacement["state"], "succeeded")
        edition = self.ledger.editions.get(replacement["edition_id"])
        self.assertFalse(edition["result"]["quality"]["publishable"])

    def test_intermediate_data_never_visible_as_report(self):
        job = self.submit()
        claimed = self.ledger.jobs.claim("w1")
        partial = self.ledger.jobs.run_claimed(claimed, "w1", max_steps=2)
        self.assertEqual(partial["state"], "running")
        self.assertEqual(partial["progress"], {"completed": 2, "total": 5})
        self.assertEqual([step["key"] for step in partial["completed_steps"]],
                         ["references", "period:current"])
        scope = partial["completed_steps"][1]["scope"]
        self.assertEqual(scope, {"kind": "period", "period": "current",
                                 "start": "2026-11-16", "end": "2026-11-22"})
        for step in partial["completed_steps"]:
            self.assertEqual(set(step), {"key", "scope", "epoch", "completed_at"})
        self.assertNotIn("result", partial)
        self.assertEqual(self.ledger.editions.list(), [])
        self.assertEqual(self.ledger.review.channels(), [])
        counts = self.ledger.publication_readiness.queue()["counts"]
        self.assertTrue(all(value == 0 for value in counts.values()))
        done = self.ledger.jobs.run_claimed(self.ledger.jobs.get(job["id"]), "w1")
        self.assertEqual(done["state"], "succeeded")
        self.assertEqual(len(self.ledger.editions.list()), 1)

    def test_sync_create_blocked_while_job_active(self):
        job = self.submit("shared")
        self.assertError("job_in_progress", self.ledger.editions.create, "shared", request_data())
        done = self.run_to_success("shared")
        edition = self.ledger.editions.create("shared", request_data())
        self.assertEqual(edition["id"], done["edition_id"])

    def test_submit_adopts_existing_edition_at_same_revision(self):
        edition = self.edition("shared")
        job = self.submit("shared")
        self.assertEqual(job["state"], "succeeded")
        self.assertEqual(job["edition_id"], edition["id"])
        self.assertEqual(job["result_digest"], edition["result_digest"])
        self.assertEqual(job["progress"]["completed"], job["progress"]["total"])
        names = [event["event"] for event in self.ledger.jobs.events(job["id"])]
        self.assertEqual(names, ["adopted"])
        replay = self.submit("shared")
        self.assertEqual(replay["id"], job["id"])

    def test_submit_conflicts_with_edition_at_other_revision(self):
        self.edition("shared")
        self.ledger.intake.ingest("alpha", "more", [synthetic_event("c1", "2026-11-18")])
        self.assertError("request_key_conflict", self.submit, "shared")
        self.assertEqual(self.ledger.jobs.list(), [])

    def test_job_edition_flows_through_review_publish_and_export(self):
        done = self.run_to_success()
        edition = self.ledger.editions.get(done["edition_id"])
        published = self.publish(edition)
        self.assertEqual(published["state"], "published")
        channel = self.ledger.review.channel("weekly")
        self.assertEqual(channel["edition_id"], edition["id"])
        bundle = self.ledger.export.bundle(edition["id"])
        self.assertTrue(self.ledger.export.verify_bundle(bundle)["ok"])
        self.assertEqual(self.ledger.export.public_channel("weekly")["result"]["periods"]["current"]
                         ["cube"]["totals"]["orders"], 3)

    def test_parallel_claim_of_single_job_has_one_winner(self):
        job = self.submit()

        def claim(owner):
            try:
                return Ledger(self.path).jobs.claim(owner)
            except LedgerError as exc:
                return exc.code

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(claim, ["w1", "w2"]))
        won = [row for row in results if isinstance(row, dict)]
        self.assertEqual(len(won), 1)
        self.assertEqual(won[0]["id"], job["id"])
        self.assertEqual(won[0]["epoch"], 1)

    def test_parallel_workers_process_each_job_exactly_once(self):
        for index in range(3):
            self.ledger.jobs.submit(f"parallel-{index}", request_data())

        def work(owner):
            return Ledger(self.path).jobs.work(owner)

        with ThreadPoolExecutor(max_workers=2) as pool:
            processed = list(pool.map(work, ["w1", "w2"]))
        self.assertEqual(sum(processed), 3)
        jobs = self.ledger.jobs.list()
        self.assertEqual(len(jobs), 3)
        for job in jobs:
            self.assertEqual(job["state"], "succeeded")
            self.assertEqual(job["attempts"], 1)
        self.assertEqual(len(self.ledger.editions.list()), 3)

    def test_job_list_filters_by_state(self):
        done = self.run_to_success("one")
        self.submit("two")
        queued = self.ledger.jobs.list("queued")
        self.assertEqual([job["request_key"] for job in queued], ["two"])
        succeeded = self.ledger.jobs.list("succeeded")
        self.assertEqual([job["request_key"] for job in succeeded], ["one"])
        self.assertError("invalid_state", self.ledger.jobs.list, "unknown")


class JobApiTests(LedgerCase):
    def setUp(self):
        super().setUp()
        self.app = create_app(self.path)
        self.client = TestClient(self.app)
        self.addCleanup(self.client.close)

    def test_api_job_lifecycle(self):
        response = self.client.post("/analysis-jobs", json={"request_key": "api-job", "request": request_data()})
        self.assertEqual(response.status_code, 200, response.text)
        job = response.json()
        self.assertEqual(job["state"], "queued")
        replay = self.client.post("/analysis-jobs", json={"request_key": "api-job", "request": request_data()})
        self.assertEqual(replay.json()["id"], job["id"])
        conflict = self.client.post("/analysis-jobs",
                                    json={"request_key": "api-job", "request": request_data(start="2026-11-17")})
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(self.client.get("/analysis-jobs/job-none").status_code, 404)
        cancelled = self.client.post(f"/analysis-jobs/{job['id']}/cancel", json={"actor": "tester"})
        self.assertEqual(cancelled.json()["state"], "cancelled")
        again = self.client.post(f"/analysis-jobs/{job['id']}/cancel", json={"actor": "tester"})
        self.assertEqual(again.json()["state"], "cancelled")
        retried = self.client.post(f"/analysis-jobs/{job['id']}/retry", json={"actor": "tester"})
        self.assertEqual(retried.json()["state"], "queued")
        self.assertEqual(self.app.state.ledger.jobs.work("api-worker"), 1)
        done = self.client.get(f"/analysis-jobs/{job['id']}").json()
        self.assertEqual(done["state"], "succeeded")
        self.assertEqual(done["progress"]["completed"], done["progress"]["total"])
        events = self.client.get(f"/analysis-jobs/{job['id']}/events").json()
        names = [event["event"] for event in events]
        self.assertEqual(names[0], "submitted")
        self.assertIn("cancelled", names)
        self.assertIn("retried", names)
        self.assertEqual(names[-1], "succeeded")
        listed = self.client.get("/analysis-jobs", params={"state": "succeeded"}).json()
        self.assertEqual(len(listed), 1)
        invalid = self.client.get("/analysis-jobs", params={"state": "unknown"})
        self.assertEqual(invalid.status_code, 422)
        edition = self.client.get(f"/editions/{done['edition_id']}")
        self.assertEqual(edition.status_code, 200)
        self.assertEqual(edition.json()["result"]["periods"]["current"]["cube"]["totals"]["orders"], 3)
