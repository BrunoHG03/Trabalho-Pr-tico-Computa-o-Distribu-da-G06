FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir grpcio grpcio-tools

COPY proto/ ./proto/
COPY recommender.py test_worker_failures.py test_replication.py ./

RUN python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/recommender.proto