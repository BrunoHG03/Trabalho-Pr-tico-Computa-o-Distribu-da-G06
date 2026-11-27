# Trabalho Prático de Computação Distribuída - Grupo 06
## Componentes do Grupo:
- Bruno Hermeto Guimarães
- Enzo de Melo Bolognani
- João Pedro Nascimento Fernandes
- Pedro Augusto Portes Araújo Cattoni
- Victor Eduardo Cruz Arrighi

## Sistema de Recomendação Distribuído e Tolerante a Falhas (gRPC + Lamport + Primary-Backup)

Protótipo acadêmico em Python 3 que demonstra chamadas RPC gRPC para recomendar filmes com filtragem colaborativa por item, ordenar atualizações em lote com relógios lógicos de Lamport e replicar o estado do coordenador no modelo Primário-Backup.

## O que a demonstração verifica

- Quatro pares de filmes são distribuídos entre dois workers por RPC.
- A lista de recomendações coincide com o cálculo local para o mesmo conjunto.
- Atualizações com carimbos lógicos são ordenadas por `(logical_time, node_id, request_id)`.
- Repetir um lote com os mesmos `request_id` não reaplica as avaliações.
- A regra do relógio de Lamport `max(local, recebido) + 1` é conferida.
- São medidas 15 execuções da recomendação distribuída e local, reportando a mediana.

O conjunto é sintético (24 avaliações, seis usuários de treinamento e quatro filmes). A medição em localhost ilustra o custo das chamadas RPC nessa amostra.

## Tratamento de falhas dos workers

1. O coordenador envia as subtarefas em paralelo. Cada tentativa tem um prazo de 2 segundos por padrão, limitado também pelo tempo restante do pedido do cliente, quando informado. O comando `python recommender.py coordinator --worker-timeout 3` inicia o coordenador com um prazo de 3 segundos por tentativa.

2. Ao receber `UNAVAILABLE` ou `DEADLINE_EXCEEDED`, o coordenador registra o erro e reenvia somente a subtarefa afetada ao próximo worker. Cada worker configurado recebe no máximo uma tentativa explícita por subtarefa. O mesmo `task_id`, conjunto de pares, versão e avaliações seguem no reenvio. Calcular similaridades é uma operação sem alteração de estado no worker, então executá-la novamente é seguro mesmo se a primeira tentativa tiver chegado a executar.

3. Antes de usar a resposta, o coordenador verifica se o ID da tarefa, o ID do worker, a versão e os pares correspondem ao pedido. Ele descarta uma resposta incompatível e reenvia a subtarefa. O ranking só é montado quando todas as subtarefas têm respostas válidas. O campo `workers_used` conta os workers distintos que forneceram essas respostas, e `pairs_dispatched` conta os pares do pedido original.

4. Se as tentativas acabam por falhas de comunicação, o cliente recebe `UNAVAILABLE`. Caso tenham ocorrido respostas incompatíveis e nenhuma resposta válida para a subtarefa, o cliente recebe `DATA_LOSS`. Um worker reiniciado pode voltar a participar nas próximas recomendações.

Cada recomendação captura a lista de avaliações e sua versão. Um lote com novas avaliações incrementa a versão uma vez. Se o lote chega durante uma recomendação, suas avaliações entram nos próximos pedidos. Isso mantém os dados iguais entre as subtarefas originais e os reenvios.

## Replicação do coordenador (Primário-Backup)

1. O ambiente suporta dois coordenadores (`coordinator-primary` na porta 50050 e `coordinator-backup` na porta 50053), além dos dois workers (`worker-a` na 50051 e `worker-b` na 50052).

2. Ao confirmar um lote em `ApplyRatingUpdates`, o coordenador primário envia um instantâneo completo (`ReplicaState`) para a réplica por meio da chamada RPC `SyncState`, contendo as avaliações acumuladas, a versão do conjunto de dados, o valor atual do relógio lógico de Lamport e os request_id já aplicados.

O cliente resiliente (`ReplicatedCoordinatorClient`) direciona os pedidos para o coordenador ativo e, ao detectar indisponibilidade (`UNAVAILABLE` ou `DEADLINE_EXCEEDED`), alterna automaticamente para o coordenador backup sem perda de estado e preservando a idempotência de requisições anteriores.

## Executar a demonstração local

Requer Python 3.10 ou superior e as portas locais 50050, 50051 e 50052 livres.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python recommender.py demo
```

No Windows PowerShell, execute com o Python do ambiente virtual:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe recommender.py demo
```

O comando inicia dois processos worker em portas distintas, levanta o serviço do coordenador, envia pedidos gRPC e executa as verificações. O programa encerra os processos dos workers ao terminar.

## Testar falhas e replicação (Testes automatizados)
Requer Python 3.10 ou superior.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python test_worker_failures.py

python test_replication.py
```

No Windows PowerShell, execute com o Python do ambiente virtual:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe test_worker_failures.py

.\.venv\Scripts\python.exe test_replication.py
```

## Demonstração com Docker Compose
Pré-requisitos:
Docker Engine (20.10+) e Docker Compose (v2+, comando `docker compose`) instalados e em execução.
Portas locais 50050, 50051, 50052 e 50053 livres.

### Cenário sem falhas:
```
docker compose up -d

docker compose ps

docker compose exec coordinator-primary python -c "import recommender_pb2 as pb; from recommender import ReplicatedCoordinatorClient, PROFILE; c = ReplicatedCoordinatorClient(('coordinator-primary:50050', 'coordinator-backup:50053')); req = pb.RecommendationRequest(user_id='target', top_k=2); req.profile.extend(pb.Rating(user_id='target', item_id=i, value=v) for _, i, v in PROFILE); res = c.Recommend(req); b1 = pb.UpdateBatch(updates=[pb.RatingUpdate(request_id='r2', node_id='node-b', logical_time=7, rating=pb.Rating(user_id='u9', item_id='D', value=4)), pb.RatingUpdate(request_id='r1', node_id='node-a', logical_time=7, rating=pb.Rating(user_id='u9', item_id='C', value=5)), pb.RatingUpdate(request_id='r3', node_id='node-a', logical_time=8, rating=pb.Rating(user_id='u8', item_id='A', value=3))]); up = c.ApplyRatingUpdates(b1); c.ApplyRatingUpdates(b1); print('VALIDATION: PASS'); print('RPC:', res.workers_used, 'worker nodes;', res.pairs_dispatched, 'similarity pairs'); print('recommendations:', ', '.join(f'{x.item_id}={x.score:.4f}' for x in res.items)); print('Lamport batch order:', list(up.ordered_request_ids), '| clock:', up.logical_clock)"

docker compose down
```

### Cenários com falhas:
```
docker compose up -d

docker compose ps

docker compose stop worker-a

docker compose ps

docker compose exec coordinator-primary python -c "import recommender_pb2 as pb; from recommender import ReplicatedCoordinatorClient, PROFILE; c = ReplicatedCoordinatorClient(('coordinator-primary:50050', 'coordinator-backup:50053')); req = pb.RecommendationRequest(user_id='target', top_k=2); req.profile.extend(pb.Rating(user_id='target', item_id=i, value=v) for _, i, v in PROFILE); res = c.Recommend(req); print('RPC:', res.workers_used, 'worker node;', res.pairs_dispatched, 'similarity pairs'); print('recommendations:', ', '.join(f'{x.item_id}={x.score:.4f}' for x in res.items))"

docker compose start worker-a

docker compose down
```

### Cenário de queda do coordenador
```
docker compose up -d

docker compose ps

docker compose exec coordinator-backup python -c "import recommender_pb2 as pb; from recommender import ReplicatedCoordinatorClient; c = ReplicatedCoordinatorClient(('coordinator-primary:50050', 'coordinator-backup:50053')); b = pb.UpdateBatch(updates=[pb.RatingUpdate(request_id='r99', node_id='node-a', logical_time=15, rating=pb.Rating(user_id='u9', item_id='C', value=5))]); up = c.ApplyRatingUpdates(b); print('Antes da queda -> Clock:', up.logical_clock, '| Ordem:', list(up.ordered_request_ids))"

docker compose stop coordinator-primary

docker compose ps

docker compose exec coordinator-backup python -c "import recommender_pb2 as pb; from recommender import ReplicatedCoordinatorClient, PROFILE; c = ReplicatedCoordinatorClient(('coordinator-primary:50050', 'coordinator-backup:50053')); b = pb.UpdateBatch(updates=[pb.RatingUpdate(request_id='r99', node_id='node-a', logical_time=15, rating=pb.Rating(user_id='u9', item_id='C', value=5))]); up = c.ApplyRatingUpdates(b); req = pb.RecommendationRequest(user_id='target', top_k=2); req.profile.extend(pb.Rating(user_id='target', item_id=i, value=v) for _, i, v in PROFILE); res = c.Recommend(req); print('RPC:', res.workers_used, 'worker nodes;', res.pairs_dispatched, 'similarity pairs'); print('recommendations:', ', '.join(f'{x.item_id}={x.score:.4f}' for x in res.items)); print('Lamport batch order:', list(up.ordered_request_ids), '| clock:', up.logical_clock)"

docker compose start coordinator-primary

docker compose down
```

### A primeira vez da execução após alteração de algum arquivo `.py` ou `.proto` executar utilizando `docker compose up -d --build` ao invés de `docker compose up -d`

## Arquivos:
- `recommender.py`: coordenador (primário e backup), worker, cliente replicado, cálculo de similaridade, Lamport e demonstração.

- `test_worker_failures.py`: testes de integração do tratamento de falhas dos workers.

- `test_replication.py`: testes de integração da replicação Primário-Backup e failover do cliente.

- `proto/recommender.proto`: definição dos serviços (CoordinatorService, WorkerService) e mensagens gRPC.

- `recommender_pb2.py` e `recommender_pb2_grpc.py`: módulos gerados pelo grpcio-tools.

- `Dockerfile` e `docker-compose.yml`: empacotamento e orquestração dos containers do sistema distribuído.

- `requirements.txt`: versões usadas para executar e regenerar os módulos Protocol Buffers.
