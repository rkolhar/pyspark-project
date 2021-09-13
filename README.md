# Hello Clue
This repo makes use of pandas library and pyspark framework to complete the tasks.

## How to run:
### Build a docker image
```bash
docker build -t pyspark-cluster:3.1.2 .
```
### Run docker-compose
```bash
docker compose up
```

### To run pyspark, exec into the master/worker container
```bash
winpty docker exec -it new_clue_spark-master_1 bash
```
or 
```bash
docker exec -it new_clue_spark-master_1 /bin/bash
```
