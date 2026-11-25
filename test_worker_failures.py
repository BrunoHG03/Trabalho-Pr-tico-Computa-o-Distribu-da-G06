import unittest
from concurrent import futures
from threading import Event

import grpc

import recommender_pb2 as pb
import recommender_pb2_grpc as rpc
from recommender import Coordinator, DATASET_VERSION, PROFILE, Worker, local_recommend


class RecordingWorker(Worker):
    def __init__(self, worker_id):
        super().__init__(worker_id)
        self.requests = []

    def CalculateSimilarities(self, request, context):
        self.requests.append(request)
        return super().CalculateSimilarities(request, context)


class DelayedWorker(Worker):
    def __init__(self, worker_id):
        super().__init__(worker_id)
        self.started = Event()
        self.release = Event()
        self.request = None

    def CalculateSimilarities(self, request, context):
        self.request = request
        self.started.set()
        self.release.wait(timeout=8)
        return super().CalculateSimilarities(request, context)


class IncompleteWorker(Worker):
    def CalculateSimilarities(self, request, context):
        reply = super().CalculateSimilarities(request, context)
        del reply.values[-1]
        return reply


class UnavailableWorker(Worker):
    def CalculateSimilarities(self, request, context):
        context.abort(grpc.StatusCode.UNAVAILABLE, "worker is temporarily unavailable")


class WorkerFailureTests(unittest.TestCase):
    def start_server(self, service, register, address="127.0.0.1:0"):
        executor = futures.ThreadPoolExecutor(max_workers=4)
        self.addCleanup(executor.shutdown)
        server = grpc.server(executor)
        register(service, server)
        port = server.add_insecure_port(address)
        self.addCleanup(lambda: server.stop(0).wait(timeout=3))
        server.start()
        return server, f"127.0.0.1:{port}"

    def connect(self, worker_a, worker_b):
        self.server_a, self.address_a = self.start_server(worker_a, rpc.add_WorkerServiceServicer_to_server)
        self.server_b, address_b = self.start_server(worker_b, rpc.add_WorkerServiceServicer_to_server)
        self.coordinator = Coordinator(
            (("worker-a", self.address_a), ("worker-b", address_b)), worker_timeout=0.5
        )
        self.addCleanup(self.coordinator.close)
        for channel in self.coordinator.channels:
            grpc.channel_ready_future(channel).result(timeout=5)

        _, address = self.start_server(self.coordinator, rpc.add_CoordinatorServiceServicer_to_server)
        channel = grpc.insecure_channel(address)
        self.addCleanup(channel.close)
        grpc.channel_ready_future(channel).result(timeout=5)
        self.client = rpc.CoordinatorServiceStub(channel)
        self.request = pb.RecommendationRequest(
            user_id="target",
            top_k=2,
            profile=[pb.Rating(user_id=user, item_id=item, value=value) for user, item, value in PROFILE],
        )
        self.expected = self.local_result()

    def local_result(self):
        return local_recommend(self.coordinator.ratings, self.request.profile, self.request.top_k)

    def assert_recommendations(self, result, workers, expected):
        self.assertEqual(result.workers_used, workers)
        self.assertEqual(result.pairs_dispatched, 4)
        self.assertEqual([(item.item_id, item.score) for item in result.items], expected)

    def test_recovers_after_worker_stops_and_uses_it_again_after_restart(self):
        survivor = RecordingWorker("worker-b")
        self.connect(Worker("worker-a"), survivor)
        self.assertTrue(self.server_a.stop(0).wait(timeout=3))

        result = self.client.Recommend(self.request, timeout=4)
        self.assert_recommendations(result, workers=1, expected=self.expected)
        self.assertEqual(sorted(len(task.pairs) for task in survivor.requests), [2, 2])

        self.start_server(Worker("worker-a"), rpc.add_WorkerServiceServicer_to_server, self.address_a)
        grpc.channel_ready_future(self.coordinator.channels[0]).result(timeout=5)
        result = self.client.Recommend(self.request, timeout=4)
        self.assert_recommendations(result, workers=2, expected=self.expected)

    def test_delayed_worker_retries_the_same_snapshot_even_if_ratings_change(self):
        slow_worker = DelayedWorker("worker-a")
        survivor = RecordingWorker("worker-b")
        self.connect(slow_worker, survivor)
        pending = self.client.Recommend.future(self.request, timeout=4)
        try:
            self.assertTrue(slow_worker.started.wait(timeout=3))
            batch = pb.UpdateBatch(updates=[pb.RatingUpdate(
                request_id="new-rating",
                node_id="test",
                logical_time=1,
                rating=pb.Rating(user_id="u1", item_id="C", value=5),
            )])
            self.client.ApplyRatingUpdates(batch, timeout=3)
            self.client.ApplyRatingUpdates(batch, timeout=3)

            result = pending.result(timeout=4)
            self.assert_recommendations(result, workers=1, expected=self.expected)
            self.assertEqual(result.dataset_version, DATASET_VERSION)
            self.assertIn(slow_worker.request, survivor.requests)
            self.assertNotEqual(self.local_result(), self.expected)
        finally:
            slow_worker.release.set()

        current = self.client.Recommend(self.request, timeout=4)
        self.assert_recommendations(current, workers=2, expected=self.local_result())
        self.assertEqual(current.dataset_version, DATASET_VERSION + 1)
        self.assertEqual(len(self.coordinator.ratings), 25)

    def test_returns_unavailable_when_both_workers_stop(self):
        self.connect(Worker("worker-a"), Worker("worker-b"))
        self.assertTrue(self.server_a.stop(0).wait(timeout=3))
        self.assertTrue(self.server_b.stop(0).wait(timeout=3))

        with self.assertRaises(grpc.RpcError) as call:
            self.client.Recommend(self.request, timeout=4)
        self.assertEqual(call.exception.code(), grpc.StatusCode.UNAVAILABLE)

    def test_reassigns_task_when_worker_returns_unavailable(self):
        self.connect(UnavailableWorker("worker-a"), Worker("worker-b"))
        result = self.client.Recommend(self.request, timeout=4)
        self.assert_recommendations(result, workers=1, expected=self.expected)

    def test_discards_incomplete_reply_and_retries_on_other_worker(self):
        self.connect(IncompleteWorker("worker-a"), Worker("worker-b"))
        result = self.client.Recommend(self.request, timeout=4)
        self.assert_recommendations(result, workers=1, expected=self.expected)

    def test_returns_data_loss_when_no_worker_produces_a_complete_reply(self):
        self.connect(IncompleteWorker("worker-a"), IncompleteWorker("worker-b"))
        with self.assertRaises(grpc.RpcError) as call:
            self.client.Recommend(self.request, timeout=4)
        self.assertEqual(call.exception.code(), grpc.StatusCode.DATA_LOSS)


if __name__ == "__main__":
    unittest.main()
