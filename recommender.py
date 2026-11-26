from __future__ import annotations

import argparse
import logging
import math
import os
import statistics
import subprocess
import sys
import time
from collections import Counter
from concurrent import futures
from threading import Lock
from typing import Iterable
from uuid import uuid4

import grpc
import recommender_pb2 as pb
import recommender_pb2_grpc as rpc

DATASET_VERSION = 1
def _load_workers():
    env_workers = os.getenv("WORKERS")
    if not env_workers:
        return (("worker-a", "127.0.0.1:50051"), ("worker-b", "127.0.0.1:50052"))
    return tuple(tuple(item.split("=", 1)) for item in env_workers.split(",") if "=" in item)

WORKERS = _load_workers()
COORDINATOR = os.getenv("COORDINATOR_ADDRESS", "127.0.0.1:50050")
BACKUP_COORDINATOR = os.getenv("BACKUP_COORDINATOR_ADDRESS", "127.0.0.1:50053")
WORKER_TIMEOUT = 2.0
RETRYABLE_WORKER_ERRORS = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}
logger = logging.getLogger(__name__)

TRAINING = (
    ("u1", "A", 5), ("u1", "B", 4), ("u1", "C", 1), ("u1", "D", 1),
    ("u2", "A", 4), ("u2", "B", 5), ("u2", "C", 1), ("u2", "D", 2),
    ("u3", "A", 1), ("u3", "B", 2), ("u3", "C", 5), ("u3", "D", 4),
    ("u4", "A", 2), ("u4", "B", 1), ("u4", "C", 4), ("u4", "D", 5),
    ("u5", "A", 5), ("u5", "B", 4), ("u5", "C", 2), ("u5", "D", 1),
    ("u6", "A", 1), ("u6", "B", 1), ("u6", "C", 5), ("u6", "D", 5),
)
PROFILE = (("target", "A", 5.0), ("target", "B", 4.0))


def cosine_co_rated(ratings: Iterable[pb.Rating], left: str, right: str) -> float:
    by_user = {}
    for rating in ratings:
        by_user.setdefault(rating.user_id, {})[rating.item_id] = rating.value
    common = [(items[left], items[right]) for items in by_user.values() if left in items and right in items]
    if not common:
        return 0.0
    dot = sum(a * b for a, b in common)
    norm_l = math.sqrt(sum(a * a for a, _ in common))
    norm_r = math.sqrt(sum(b * b for _, b in common))
    return dot / (norm_l * norm_r) if norm_l and norm_r else 0.0


def local_recommend(ratings, profile, top_k):
    known = {r.item_id for r in profile}
    candidates = sorted({r.item_id for r in ratings} - known)
    profile_map = {r.item_id: r.value for r in profile}
    predicted = []
    for candidate in candidates:
        sims = {item: cosine_co_rated(ratings, candidate, item) for item in profile_map}
        denom = sum(abs(value) for value in sims.values())
        if denom:
            score = sum(sims[item] * rating for item, rating in profile_map.items()) / denom
            predicted.append((candidate, score))
    return sorted(predicted, key=lambda pair: (-pair[1], pair[0]))[:top_k]


class LamportClock:
    def __init__(self, value: int = 0): self.value = value
    def tick(self) -> int:
        self.value += 1
        return self.value
    def receive(self, remote: int) -> int:
        self.value = max(self.value, remote) + 1
        return self.value


class Worker(rpc.WorkerServiceServicer):
    def __init__(self, worker_id: str): self.worker_id = worker_id
    def CalculateSimilarities(self, request, context):
        reply = pb.SimilarityReply(task_id=request.task_id, dataset_version=request.dataset_version, worker_id=self.worker_id)
        for pair in request.pairs:
            val = cosine_co_rated(request.ratings, pair.candidate_item, pair.profile_item)
            reply.values.add(candidate_item=pair.candidate_item, profile_item=pair.profile_item, value=val)
        return reply


class SimilarityTaskError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


class Coordinator(rpc.CoordinatorServiceServicer):
    def __init__(self, worker_targets=WORKERS, worker_timeout=WORKER_TIMEOUT,
                 backup_address: str | None = None, sync_timeout: float = 1.0):
        if not math.isfinite(worker_timeout) or worker_timeout <= 0:
            raise ValueError("worker timeout must be a positive, finite number of seconds")
        self.worker_targets = tuple(worker_targets)
        self.worker_timeout = worker_timeout
        self.channels = [grpc.insecure_channel(target) for _, target in self.worker_targets]
        self.stubs = [rpc.WorkerServiceStub(ch) for ch in self.channels]
        self.backup_address = backup_address
        self.sync_timeout = sync_timeout
        self.backup_channel = grpc.insecure_channel(backup_address) if backup_address else None
        self.backup_stub = rpc.CoordinatorServiceStub(self.backup_channel) if self.backup_channel else None
        self.ratings = [pb.Rating(user_id=u, item_id=i, value=v) for u, i, v in TRAINING]
        self.dataset_version = DATASET_VERSION
        self.state_lock = Lock()
        self.clock = LamportClock()
        self.applied = set()

    def close(self):
        for ch in self.channels: ch.close()
        if self.backup_channel is not None:
            self.backup_channel.close()

    def _calculate_chunk(self, index, task, context):
        expected_pairs = Counter((pair.candidate_item, pair.profile_item) for pair in task.pairs)
        invalid_reply = False
        for offset in range(len(self.stubs)):
            worker_index = (index + offset) % len(self.stubs)
            worker_id, _ = self.worker_targets[worker_index]
            remaining = context.time_remaining()
            if not context.is_active() or (remaining is not None and remaining <= 0):
                raise SimilarityTaskError(grpc.StatusCode.CANCELLED, "recommendation no longer active")
            timeout = self.worker_timeout if remaining is None else min(self.worker_timeout, remaining)
            try:
                reply = self.stubs[worker_index].CalculateSimilarities(task, timeout=timeout)
            except grpc.RpcError as error:
                code = error.code()
                if code not in RETRYABLE_WORKER_ERRORS:
                    raise SimilarityTaskError(code, f"{worker_id} failed to calculate similarities") from error
                logger.warning("Task %s failed on %s (%s)", task.task_id, worker_id, code.name)
                continue

            received_pairs = Counter((value.candidate_item, value.profile_item) for value in reply.values)
            if (
                reply.task_id != task.task_id
                or reply.dataset_version != task.dataset_version
                or reply.worker_id != worker_id
                or received_pairs != expected_pairs
            ):
                invalid_reply = True
                logger.warning("Task %s received an invalid reply from %s", task.task_id, worker_id)
                continue
            if offset:
                logger.info("Task %s recovered on %s", task.task_id, worker_id)
            return reply

        if invalid_reply:
            raise SimilarityTaskError(grpc.StatusCode.DATA_LOSS, "no valid worker reply for a similarity task")
        raise SimilarityTaskError(grpc.StatusCode.UNAVAILABLE, "no worker completed a similarity task")

    def Recommend(self, request, context):
        if not request.profile:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "profile must contain at least one rating")
        with self.state_lock:
            ratings = tuple(self.ratings)
            dataset_version = self.dataset_version
        known = {r.item_id for r in request.profile}
        items = sorted({r.item_id for r in ratings})
        candidates = [item for item in items if item not in known]
        pairs = [(candidate, profile.item_id) for candidate in candidates for profile in request.profile]
        if not pairs:
            return pb.RecommendationReply(dataset_version=dataset_version)
        if not self.stubs:
            context.abort(grpc.StatusCode.UNAVAILABLE, "no workers configured")
        # Split the independent item pairs as evenly as possible among configured worker nodes.
        chunks = [pairs[i::len(self.stubs)] for i in range(len(self.stubs))]
        calls = []
        recommendation_id = uuid4().hex
        for index, chunk in enumerate(chunks):
            if not chunk:
                continue
            req = pb.SimilarityRequest(
                task_id=f"{recommendation_id}-{index}", dataset_version=dataset_version, ratings=ratings
            )
            req.pairs.extend(pb.ItemPair(candidate_item=a, profile_item=b) for a, b in chunk)
            calls.append((index, req))
        try:
            with futures.ThreadPoolExecutor(max_workers=len(calls)) as pool:
                pending = [pool.submit(self._calculate_chunk, index, req, context) for index, req in calls]
                replies = [future.result() for future in pending]
        except SimilarityTaskError as error:
            context.abort(error.code, str(error))

        similarities = {}
        for reply in replies:
            for value in reply.values:
                similarities[(value.candidate_item, value.profile_item)] = value.value
        predicted = []
        profile_map = {r.item_id: r.value for r in request.profile}
        for candidate in candidates:
            terms = [(similarities[(candidate, item)], rating) for item, rating in profile_map.items()]
            denom = sum(abs(sim) for sim, _ in terms)
            if denom:
                score = sum(sim * rating for sim, rating in terms) / denom
                predicted.append((candidate, score))
        predicted.sort(key=lambda pair: (-pair[1], pair[0]))
        result = pb.RecommendationReply(
            workers_used=len({reply.worker_id for reply in replies}),
            pairs_dispatched=len(pairs),
            dataset_version=dataset_version,
        )
        result.items.extend(pb.RecommendedItem(item_id=item, score=score) for item, score in predicted[:request.top_k])
        return result

    def ApplyRatingUpdates(self, request, context):
        ordered = sorted(request.updates, key=lambda u: (u.logical_time, u.node_id, u.request_id))
        snapshot_to_sync = None
        with self.state_lock:
            changed = False
            for update in ordered:
                self.clock.receive(update.logical_time)
                if update.request_id not in self.applied:
                    self.applied.add(update.request_id)
                    self.ratings.append(update.rating)
                    changed = True
            if changed:
                self.dataset_version += 1
            if self.backup_stub is not None and ordered:
                snapshot_to_sync = pb.ReplicaState(
                    dataset_version=self.dataset_version,
                    logical_clock=self.clock.value,
                    ratings=list(self.ratings),
                    applied_request_ids=sorted(self.applied),
                )
            result = pb.UpdateResult(
                applied=bool(ordered),
                logical_clock=self.clock.value,
                ordered_request_ids=[update.request_id for update in ordered],
            )
        if snapshot_to_sync is not None:
            try:
                self.backup_stub.SyncState(snapshot_to_sync, timeout=self.sync_timeout)
            except grpc.RpcError as error:
                logger.warning("Replication to backup %s failed (%s)", self.backup_address, error.code().name)
        return result

    def SyncState(self, request, context):
            with self.state_lock:
                if request.dataset_version >= self.dataset_version:
                    self.ratings = list(request.ratings)
                    self.dataset_version = request.dataset_version
                    self.clock.value = max(self.clock.value, request.logical_clock)
                    self.applied.update(request.applied_request_ids)
                    logger.info(
                        "Replica synchronized: dataset_version=%d, logical_clock=%d, ratings=%d",
                        self.dataset_version,
                        self.clock.value,
                        len(self.ratings),
                    )
                return pb.SyncAck(ok=True, dataset_version=self.dataset_version)

class ReplicatedCoordinatorClient:
    def __init__(self, endpoints: Iterable[str]):
        self.endpoints = tuple(endpoints)
        if not self.endpoints:
            raise ValueError("at least one coordinator endpoint is required")
        self.channels = [grpc.insecure_channel(addr) for addr in self.endpoints]
        self.stubs = [rpc.CoordinatorServiceStub(ch) for ch in self.channels]
        self.active_index = 0

    def close(self):
        for ch in self.channels:
            ch.close()

    def _call_with_failover(self, method_name: str, request, timeout: float):
        last_error = None
        for offset in range(len(self.stubs)):
            idx = (self.active_index + offset) % len(self.stubs)
            try:
                response = getattr(self.stubs[idx], method_name)(request, timeout=timeout)
                if idx != self.active_index:
                    logger.info("Client promoted replica %s as active coordinator", self.endpoints[idx])
                    self.active_index = idx
                return response
            except grpc.RpcError as error:
                if error.code() not in RETRYABLE_WORKER_ERRORS:
                    raise
                logger.warning("Coordinator %s unavailable (%s), trying next replica", self.endpoints[idx], error.code().name)
                last_error = error
        assert last_error is not None
        raise last_error

    def Recommend(self, request: pb.RecommendationRequest, timeout: float = 5.0) -> pb.RecommendationReply:
        return self._call_with_failover("Recommend", request, timeout=timeout)

    def ApplyRatingUpdates(self, request: pb.UpdateBatch, timeout: float = 5.0) -> pb.UpdateResult:
        return self._call_with_failover("ApplyRatingUpdates", request, timeout=timeout)
        


def run_worker(worker_id, address):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    rpc.add_WorkerServiceServicer_to_server(Worker(worker_id), server)
    server.add_insecure_port(address)
    server.start()
    print(f"{worker_id} listening at {address}", flush=True)
    server.wait_for_termination()


def run_coordinator(address=COORDINATOR, worker_timeout=WORKER_TIMEOUT, backup_address=None):
    service = Coordinator(worker_timeout=worker_timeout, backup_address=backup_address)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    rpc.add_CoordinatorServiceServicer_to_server(service, server)
    server.add_insecure_port(address)
    server.start()
    role = f"primary (backup={backup_address})" if backup_address else "standalone/replica"
    print(f"coordinator [{role}] listening at {address}", flush=True)
    try: server.wait_for_termination()
    finally:
        service.close()


def run_demo():
    env = os.environ.copy()
    root = os.path.dirname(os.path.abspath(__file__))
    old = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = root + (os.pathsep + old if old else "")
    processes = []
    try:
        for worker_id, address in WORKERS:
            p = subprocess.Popen([sys.executable, os.path.abspath(__file__), "worker", worker_id, address], env=env, stdout=subprocess.DEVNULL)
            processes.append(p)
        ready = False
        for _ in range(100):
            if not all(p.poll() is None for p in processes): break
            channels = [grpc.insecure_channel(addr) for _, addr in WORKERS]
            ready = True
            for ch in channels:
                try: grpc.channel_ready_future(ch).result(timeout=.1)
                except grpc.FutureTimeoutError: ready = False
                finally: ch.close()
            if ready: break
            time.sleep(.05)
        if not ready: raise RuntimeError("workers did not become ready")
        service = Coordinator()
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
        rpc.add_CoordinatorServiceServicer_to_server(service, server)
        server.add_insecure_port(COORDINATOR); server.start()
        try:
            ch = grpc.insecure_channel(COORDINATOR)
            grpc.channel_ready_future(ch).result(timeout=5)
            client = rpc.CoordinatorServiceStub(ch)
            request = pb.RecommendationRequest(user_id="target", top_k=2)
            request.profile.extend(pb.Rating(user_id="target", item_id=i, value=v) for _, i, v in PROFILE)
            result = client.Recommend(request, timeout=10)
            profile = [pb.Rating(user_id="target", item_id=i, value=v) for _, i, v in PROFILE]
            expected = local_recommend(service.ratings, profile, 2)
            assert [(x.item_id, round(x.score, 10)) for x in result.items] == [(i, round(s, 10)) for i, s in expected], "distributed output differs from local baseline"
            assert result.pairs_dispatched == 4 and result.workers_used == 2
            batch1 = pb.UpdateBatch()
            batch1.updates.extend([
                pb.RatingUpdate(request_id="r2", node_id="node-b", logical_time=7, rating=pb.Rating(user_id="u9", item_id="D", value=4)),
                pb.RatingUpdate(request_id="r1", node_id="node-a", logical_time=7, rating=pb.Rating(user_id="u9", item_id="C", value=5)),
                pb.RatingUpdate(request_id="r3", node_id="node-a", logical_time=8, rating=pb.Rating(user_id="u8", item_id="A", value=3)),
            ])
            update_result = client.ApplyRatingUpdates(batch1, timeout=5)
            assert list(update_result.ordered_request_ids) == ["r1", "r2", "r3"]
            count_after_first_batch = len(service.ratings)
            client.ApplyRatingUpdates(batch1, timeout=5)
            assert len(service.ratings) == count_after_first_batch, "duplicate request IDs were applied more than once"
            clock = LamportClock(4)
            assert clock.receive(9) == 10 and clock.tick() == 11
            def ms(fn, n=15):
                vals=[]
                for _ in range(n):
                    t=time.perf_counter(); fn(); vals.append((time.perf_counter()-t)*1000)
                return statistics.median(vals)
            distributed_ms = ms(lambda: client.Recommend(request, timeout=10))
            local_ms = ms(lambda: local_recommend(service.ratings, profile, 2))
            print("VALIDATION: PASS")
            print(f"dataset: {len(TRAINING)} training ratings, {len({r[1] for r in TRAINING})} items across 6 training users; profile: 2 ratings; 3 update events in Lamport check")
            print(f"RPC: {result.workers_used} worker nodes; {result.pairs_dispatched} similarity pairs; result checksums match local baseline")
            print("recommendations: " + ", ".join(f"{x.item_id}={x.score:.4f}" for x in result.items))
            print(f"Lamport batch order: {', '.join(update_result.ordered_request_ids)}; clock after first merge: {update_result.logical_clock}; duplicate batch idempotency: PASS; receive(9) from 4 -> 10, tick -> 11")
            print(f"median latency over 15 runs: distributed end-to-end {distributed_ms:.3f} ms; local end-to-end recommendation {local_ms:.3f} ms")
            print("note: the measurements use a tiny synthetic dataset on localhost and do not establish a speedup")
            ch.close(); server.stop(0); service.close()
        finally:
            server.stop(0)
            service.close()
    finally:
        for p in processes: p.terminate()
        for p in processes:
            try: p.wait(timeout=2)
            except subprocess.TimeoutExpired: p.kill()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser=argparse.ArgumentParser(); sub=parser.add_subparsers(dest="cmd", required=True)
    w=sub.add_parser("worker"); w.add_argument("worker_id"); w.add_argument("address")
    c=sub.add_parser("coordinator"); c.add_argument("--address", default=COORDINATOR)
    c.add_argument("--worker-timeout", type=float, default=WORKER_TIMEOUT, help="RPC timeout per worker in seconds")
    c.add_argument("--backup-address", default=None, help="Backup replica coordinator address")
    sub.add_parser("demo")
    args=parser.parse_args()
    if args.cmd == "worker": run_worker(args.worker_id, args.address)
    elif args.cmd == "coordinator": run_coordinator(args.address, args.worker_timeout, args.backup_address)
    else: run_demo()
