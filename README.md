# Streaming Data Pipeline

## Description
This project serves as an example for teaching the HWE Course at 1904Labs. 
1. A Kafka producer publishes to the kafka topic `reviews`. 
2. A spark streaming application consumes reviews from the kafka topic. Within each review is a `customer_id`. 
3. The Spark streaming application joins each review with a record retrieved from Hbase, and uses this customer_ic to make that join.
4. Spark streaming stores this enriched record in HDFS.
5. Hive is used to query the data from hdfs.


## Architecture
![GitHub Logo](/images/architecture.png)