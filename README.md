# Hello Clue
This repo makes use of pandas library and pyspark framework to complete the tasks.
#### Spark version used: 3.1.2
#### Hadoop version used: 2.7


## Documentation:
### Task 1 - fix the corrupt json file:
Pandas and regex packages are used to fix the json. The fixed json file is then written as a parquet file in data folder - fixed_json.parquet.
If the data were to scale higher, I would create a udf with spark function to still have the flexibility to use regex.

### Task 2 - Aggregating multiple device per user:
 Assumption: Parquet output format is expandable to device_model_3 and created_at_3
##### 1.In the given json file, if a particular user id  were to use 3 device models to access the clue app(let's say, iPhone 5, iPhone6, iPhone5s) then the parquet file would have an extra column device_model_3.
#### 2. If the model is same, but has different created timestamps, I have again created an extra column for created_at_3. Another option would have been to only take latest created at timestamp.

This task also makes use of pandas to aggregate users and pyarrow library to convert the output to parquet file.


### Task 3 - Computing sleep range for data source 2 to be integrated with data source 1:
This task makes use of pyspark and udf function to compute time difference between endTime and startTime, computes the sleep range using pyspark's bewteen function. The result is stored in a parquet file.


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

### What to expect:
Docker compose is set up and runs through the application. However, in the last step while storing the results to parquet, sometimes, I ran into Py4JJavaError: An error occurred while calling o106.save. 
This is not a consistent behaviour. The output file is stored under data folder.

### What I could have done better:
- Test cases
- Take care of the formats of timestamps in json file.
- And of course, try to fix the inconsistent behaviour when using docker compose.
