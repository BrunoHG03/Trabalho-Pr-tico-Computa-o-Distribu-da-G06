import unittest
from concurrent import futures

import grpc

import recommender_pb2 as pb
import recommender_pb2_grpc as rpc
from recommender import (
    Coordinator,
    DATASET_VERSION,
    PROFILE,
    ReplicatedCoordinatorClient,
    Worker,
    local_recommend,
)


class ReplicationTests(unittest.TestCase):
    def start_server(self, service, register, address="127.0.0.1:0"):
        executor = futures.ThreadPoolExecutor(max_workers=4)
        self.addCleanup(executor.shutdown)
        server = grpc.server(executor)
        register(service, server)
        port = server.add_insecure_port(address)
        self.addCleanup(lambda: server.stop(0).wait(timeout=3))
        server.start()
        return server, f"127.0.0.1:{port}"

    def setUp(self):
        _, addr_a = self.start_server(Worker("worker-a"), rpc.add_WorkerServiceServicer_to_server)
        _, addr_b = self.start_server(Worker("worker-b"), rpc.add_WorkerServiceServicer_to_server)
        workers = (("worker-a", addr_a), ("worker-b", addr_b))

        self.backup = Coordinator(workers, worker_timeout=0.5)
        self.addCleanup(self.backup.close)
        self.backup_server, self.backup_addr = self.start_server(
            self.backup, rpc.add_CoordinatorServiceServicer_to_server
        )

        self.primary = Coordinator(workers, worker_timeout=0.5, backup_address=self.backup_addr)
        self.addCleanup(self.primary.close)
        self.primary_server, self.primary_addr = self.start_server(
            self.primary, rpc.add_CoordinatorServiceServicer_to_server
        )

        self.client = ReplicatedCoordinatorClient([self.primary_addr, self.backup_addr])
        self.addCleanup(self.client.close)

        self.request = pb.RecommendationRequest(
            user_id="target",
            top_k=2,
            profile=[pb.Rating(user_id=u, item_id=i, value=v) for u, i, v in PROFILE],
        )

    def test_state_is_replicated_and_survives_primary_failure(self):
        batch = pb.UpdateBatch(updates=[
            pb.RatingUpdate(
                request_id="rep-1",
                node_id="node-a",
                logical_time=5,
                rating=pb.Rating(user_id="u9", item_id="C", value=5.0),
            )
        ])
        res = self.client.ApplyRatingUpdates(batch, timeout=3)
        self.assertTrue(res.applied)
        self.assertEqual(self.backup.dataset_version, DATASET_VERSION + 1)
        self.assertEqual(self.backup.clock.value, 6)
        self.assertIn("rep-1", self.backup.applied)

        self.assertTrue(self.primary_server.stop(0).wait(timeout=3))

        rec = self.client.Recommend(self.request, timeout=4)
        expected = local_recommend(self.primary.ratings, self.request.profile, self.request.top_k)
        self.assertEqual(rec.dataset_version, DATASET_VERSION + 1)
        self.assertEqual(
            [(x.item_id, round(x.score, 8)) for x in rec.items],
            [(i, round(s, 8)) for i, s in expected],
        )

        res_dup = self.client.ApplyRatingUpdates(batch, timeout=3)
        self.assertEqual(len(self.backup.ratings), 25)
        self.assertEqual(res_dup.logical_clock, 7)

    def test_primary_continues_operating_if_backup_is_unavailable(self):
        self.assertTrue(self.backup_server.stop(0).wait(timeout=3))
        batch = pb.UpdateBatch(updates=[
            pb.RatingUpdate(
                request_id="rep-2",
                node_id="node-b",
                logical_time=3,
                rating=pb.Rating(user_id="u10", item_id="D", value=4.0),
            )
        ])
        res = self.client.ApplyRatingUpdates(batch, timeout=3)
        self.assertTrue(res.applied)
        self.assertEqual(self.primary.dataset_version, DATASET_VERSION + 1)


if __name__ == "__main__":
    unittest.main()