# Trabalho Prático de Computação Distribuída - Grupo 06
## Sistema de Recomendação Distribuído e Tolerante a Falhas (gRPC + Lamport + Primary-Backup)

Protótipo acadêmico em Python 3 que demonstra chamadas RPC gRPC para recomendar filmes com filtragem colaborativa por item, ordenar atualizações em lote com relógios lógicos de Lamport e replicar o estado do coordenador no modelo Primário-Backup.

## Executar a demonstração local

Requer Python 3.10 ou superior e as portas locais 50050, 50051 e 50052 livres.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python recommender.py demo ```

No Windows PowerShell, execute com o Python do ambiente virtual:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe recommender.py demo
```

# SEM FALHAS:
docker compose up -d

docker compose ps

docker compose exec coordinator-primary python -c "import recommender_pb2 as pb; from recommender import ReplicatedCoordinatorClient, PROFILE; c = ReplicatedCoordinatorClient(('coordinator-primary:50050', 'coordinator-backup:50053')); req = pb.RecommendationRequest(user_id='target', top_k=2); req.profile.extend(pb.Rating(user_id='target', item_id=i, value=v) for _, i, v in PROFILE); res = c.Recommend(req); b1 = pb.UpdateBatch(updates=[pb.RatingUpdate(request_id='r2', node_id='node-b', logical_time=7, rating=pb.Rating(user_id='u9', item_id='D', value=4)), pb.RatingUpdate(request_id='r1', node_id='node-a', logical_time=7, rating=pb.Rating(user_id='u9', item_id='C', value=5)), pb.RatingUpdate(request_id='r3', node_id='node-a', logical_time=8, rating=pb.Rating(user_id='u8', item_id='A', value=3))]); up = c.ApplyRatingUpdates(b1); c.ApplyRatingUpdates(b1); print('VALIDATION: PASS'); print('RPC:', res.workers_used, 'worker nodes;', res.pairs_dispatched, 'similarity pairs'); print('recommendations:', ', '.join(f'{x.item_id}={x.score:.4f}' for x in res.items)); print('Lamport batch order:', list(up.ordered_request_ids), '| clock:', up.logical_clock)"

docker compose down


# COM FALHAS:
docker compose up -d

docker compose ps

docker compose stop worker-a

docker compose ps

docker compose exec coordinator-primary python -c "import recommender_pb2 as pb; from recommender import ReplicatedCoordinatorClient, PROFILE; c = ReplicatedCoordinatorClient(('coordinator-primary:50050', 'coordinator-backup:50053')); req = pb.RecommendationRequest(user_id='target', top_k=2); req.profile.extend(pb.Rating(user_id='target', item_id=i, value=v) for _, i, v in PROFILE); res = c.Recommend(req); print('RPC:', res.workers_used, 'worker node;', res.pairs_dispatched, 'similarity pairs'); print('recommendations:', ', '.join(f'{x.item_id}={x.score:.4f}' for x in res.items))"

docker compose start worker-a

docker compose down

# REPLICA:
docker compose up -d

docker compose ps

docker compose exec coordinator-backup python -c "import recommender_pb2 as pb; from recommender import ReplicatedCoordinatorClient; c = ReplicatedCoordinatorClient(('coordinator-primary:50050', 'coordinator-backup:50053')); b = pb.UpdateBatch(updates=[pb.RatingUpdate(request_id='r99', node_id='node-a', logical_time=15, rating=pb.Rating(user_id='u9', item_id='C', value=5))]); up = c.ApplyRatingUpdates(b); print('Antes da queda -> Clock:', up.logical_clock, '| Ordem:', list(up.ordered_request_ids))"

docker compose stop coordinator-primary

docker compose ps

docker compose exec coordinator-backup python -c "import recommender_pb2 as pb; from recommender import ReplicatedCoordinatorClient, PROFILE; c = ReplicatedCoordinatorClient(('coordinator-primary:50050', 'coordinator-backup:50053')); b = pb.UpdateBatch(updates=[pb.RatingUpdate(request_id='r99', node_id='node-a', logical_time=15, rating=pb.Rating(user_id='u9', item_id='C', value=5))]); up = c.ApplyRatingUpdates(b); req = pb.RecommendationRequest(user_id='target', top_k=2); req.profile.extend(pb.Rating(user_id='target', item_id=i, value=v) for _, i, v in PROFILE); res = c.Recommend(req); print('RPC:', res.workers_used, 'worker nodes;', res.pairs_dispatched, 'similarity pairs'); print('recommendations:', ', '.join(f'{x.item_id}={x.score:.4f}' for x in res.items)); print('Lamport batch order:', list(up.ordered_request_ids), '| clock:', up.logical_clock)"

docker compose start coordinator-primary

docker compose down
