# Current Code

## To Run

#### install prereqs

```sh
pip install -r requirements.txt
```

#### Run redpanda in docker

```sh
docker run -d --pull=always --name=redpanda-1 --rm \
-p 8081:8081 \
-p 8082:8082 \
-p 9092:9092 \
-p 9644:9644 \
docker.redpanda.com/vectorized/redpanda:latest \
redpanda start \
--overprovisioned \
--smp 1  \
--memory 1G \
--reserve-memory 0M \
--node-id 0 \
--check=false
```

#### Start the load data dataflow in one terminal

```sh
python load_data.py
```

#### Start the dataflow in another terminal

```sh
python dataflow.py
```
