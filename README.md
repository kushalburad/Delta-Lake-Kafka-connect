# Delta Lake 
![img1](https://user-images.githubusercontent.com/53971225/149649712-fe407d88-8acc-4abb-9ff9-89e7425c42a9.png)


# Introduction 
In this project, we have used Delta Lake for storing streaming data which was extracted using Osquery. Delta Lake is an open-source storage layer that brings reliability to data lakes. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Initially, we will be publishing data extracted by Osquery to Kafka topics in a streaming fashion. Furthermore, we will be consuming data present in the Kafka topic using spark and modifying the schema of the table for increasing the efficiency of delta lake. Finally, we will push the modified data into delta Lake. 
What is Osquery?
Osquery is the host monitoring daemon that allows you to schedule queries and record OS state changes. The daemon aggregates query results over time and generates logs, which indicate state change according to each query. The daemon also uses OS eventing APIs to record monitored file and directory changes, hardware events, network events, and more.

# What is Kafka?
 
Apache Kafka is a community distributed event streaming platform capable of handling trillions of events a day. Initially conceived as a messaging queue, Kafka is based on an abstraction of a distributed commit log. Since being created and open-sourced by LinkedIn in 2011, Kafka has quickly evolved from messaging queue to a full-fledged event streaming platform.
Kafka is primarily used to build real-time streaming data pipelines and applications that adapt to the data streams. It combines messaging, storage, and stream processing to allow storage and analysis of both historical and real-time data.

# What is Delta Lake?

Delta Lake is an open-source storage layer that brings reliability to data lakes. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Delta Lake runs on top of your existing data lake and is fully compatible with Apache Spark APIs.


# Advantages of Delta Lake 

ACID Transactions

Data lakes typically have multiple data pipelines reading and writing data concurrently, and data engineers have to go through a tedious process to ensure data integrity, due to the lack of transactions. Delta Lake brings ACID transactions to your data lakes. It provides serializability, the strongest level of isolation level. Learn more at Diving into Delta Lake: Unpacking the Transaction Log.

Scalable Metadata Handling

In big data, even the metadata itself can be "big data." Delta Lake treats metadata just like data, leveraging Spark's distributed processing power to handle all its metadata. As a result, Delta Lake can handle petabyte-scale tables with billions of partitions and files at ease.

Time Travel (data versioning)

Delta Lake provides snapshots of data enabling developers to access and revert to earlier versions of data for audits, rollbacks or to reproduce experiments. Learn more in Introducing Delta Lake Time Travel for Large Scale Data Lakes.

Open Format

All data in Delta Lake is stored in Apache Parquet format enabling Delta Lake to leverage the efficient compression and encoding schemes that are native to Parquet.

Z-Ordering

Z-Ordering is a technique to colocate related information in the same set of files. This co-locality is automatically used by Delta Lake data-skipping algorithms to dramatically reduce the amount of data that needs to be read. 

<img width="657" alt="optimized-writes" src="https://user-images.githubusercontent.com/53971225/149659107-02665800-407d-4595-92b1-8efefd5a1c7a.png">

Unified Batch and Streaming Source and Sink

A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box.

Schema Enforcement
Delta Lake provides the ability to specify your schema and enforce it. This helps ensure that the data types are correct and required columns are present, preventing bad data from causing data corruption. For more information, refer to Diving Into Delta Lake: Schema Enforcement & Evolution.

Schema Evolution

Big data is continuously changing. Delta Lake enables you to make changes to a table schema that can be applied automatically, without the need for cumbersome DDL. For more information, refer to Diving Into Delta Lake: Schema Enforcement & Evolution.

Audit History

Delta Lake transaction log records details about every change made to data providing a full audit trail of the changes.

# Delta lake Kafka integration using spark
We will be using Spark for consuming data from Kafka topics in streaming format. Furthermore, we will modify the streaming data present in the Kafka topic by extracting the schema from the data and converting the data into table format. Finally, we will push the modified data into Delta Lake for further use. 
 
# Result 
After completing our project, we can say that Delta Lake supports streaming Osquery data furthermore, It is beneficial to use Delta Lake as it provides added benefits over a typical data lake like ACID transactions, time travel and used a Parquet file format which enhances Delta Lake performance as it is efficient in reading Data in less time as it is columnar storage and minimises latency and increases data security as data is not human-readable


# Conclusion 
Delta Lake can store streaming Osquery data provided by Kafka topics. Furthermore, Delta Lake provides added benefits over a typical data lake, such as ACID transactions and time travel (data versioning), which can be leveraged by data engineers. Moreover, Delta Lake is deeply integrated with Spark Structured Streaming through reading stream and write stream and Delta Lake overcomes many of the limitations typically associated with streaming systems and files, like maintaining “exactly-once” processing with more than one stream (or concurrent batch jobs) and Efficiently discovering which files are new when using files as the source for a stream. Furthermore, Delta Lake used a Parquet file format which further enhances Delta Lake's performance. Parquet file format is efficient in reading Data in less time as it is columnar storage and minimises latency.


# Steps to run the program

### 1 Install and run Kafka 

Commands to run kafka 

bin/zookeeper-server-start.sh config/zookeeper.properties

./bin/kafka-server-start.sh config/server.properties

### 2 Run Osquery by using Osquery. conf and Osquery.flags present in then the project 

Command to run osquery and send logs into Kafka topic. 

sudo systemctl start osqueryd.

Check the status using the below command. 

sudo systemctl status osqueryd

### 3 Run getstreamingdata.scala using sbt packages present in the project 

Create a sbt project and configure sbt file using build.sbt file present in the project.

Run the getstreamingdata.scala program for storing streaming data present in Kafka topic into delta lake file format.






