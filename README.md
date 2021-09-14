# Hello Clue
This repo makes use of pandas library and pyspark framework to complete the tasks.
### Spark version used: 3.1.2
### Hadoop version used: 2.7

## How to run:
### Build a docker image
```bash
docker build -t pyspark_cluster:3.1.2 .
```
### Run docker-compose
```bash
docker compose up
```

### To run pyspark, exec into the master/worker container
```bash
winpty docker exec -it new_clue_spark-master_1 bash

```

### spark submit
```bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077  /opt/spark-app/main.py
```