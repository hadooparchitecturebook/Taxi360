Cluster Setup
-------------

The Taxi360 demo requires a CDH cluster with the following services installed and running:
* Kudu
* Impala-Kudu
* Solr (ensure sufficient number of servers)
* YARN
* Spark on YARN
* HDFS
* Kafka
* Hue

Build the Demo and Upload to Cluster
------------------------------------

Build the demo code
-------------------
    mvn clean package

Upload Artifacts to the Cluster
-------------------------------

The application JAR built in the previous step and required data files should be uploaded to a gateway node on the cluster. This should be a host configured as a client to the Hadoop cluster:

* target/Taxi360
* data/yellow_tripdata_2009-01.10000.csv
* solr/taxi360/schema.xml

Set Up Kafka
------------

Log into the gateway node and create the Kafka topic used for the example. Note that you may need to adjust the --replication-factor parameter based on the number of Kafka brokers in the cluster:

    kafka-topics --zookeeper ZKHOST:ZKPORT --partition 2 --replication-factor 2 --create --topic taxi-trip-input

Where ZKHOST:ZKPORT is the host and port number for a ZooKeeper server

You can test the successful creation of the topic with the following:

    kafka-topics --zookeeper ZKHOST:ZKPORT --list

Set up Solr
-----------

Log into the gateway node and execute the following steps:

    solrctl instancedir --generate taxi-trip-collection
    cp schema.xml taxi-trip-collection/conf/
    solrctl instancedir --create taxi-trip-collection taxi-trip-collection
    solrctl collection --create taxi-trip-collection -s 3 -r 2 -m 3

Create Kudu Tables
------------------

Execute the following DDL scripts to create the required Kudu objects. These can be executed via the HUE Impala Query interface:

* kudu/taxi360/create_ny_taxi_trip_table.impala.sql
* kudu/taxi360/create_ny_taxi_entity_table.impala.sql

Make sure to replace "\<ReplaceMeWithKuduMasterHost\>" with the hostname for the Kudu master in each script.

Create the HBase table:
-----------------------

Log into the gateway node and execute the following step:

hadoop jar Taxi360.jar com.hadooparchitecturebook.taxi360.setup.hbase.CreateSaltedTable taxi-trip f 6 6 /opt/cloudera/parcels/CDH/lib/hbase/conf/

Executing
---------

**Kafka:**

    java -cp Taxi360.jar com.hadooparchitecturebook.taxi360.common.CsvKafkaPublisher <brokerList> <topicName> <dataFolderOrFile> <sleepPerRecord> <acks> <linger.ms> <producer.type> <batch.size> <salts>

For example:

    java -cp Taxi360.jar com.hadooparchitecturebook.taxi360.common.CsvKafkaPublisher KAFKA_BROKER_1:9092,KAFKA_BROKER_2:9092 taxi-trip-input yellow_tripdata_2009-01.10000.csv 10 0 10 async 1000 100

**Run Spark to Solr:**

    spark-submit --class com.hadooparchitecturebook.taxi360.streaming.ingestion.solr.SparkStreamingTaxiTripToSolR --master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 Taxi360.jar <KafkaBrokerList> <kafkaTopicList> <checkpointDir> <numberOfSeconds> <runLocal> <solrCollection> <zkHost:zkPort>

For example:

    spark-submit --class com.hadooparchitecturebook.taxi360.streaming.ingestion.solr.SparkStreamingTaxiTripToSolR --master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 Taxi360.jar KAFKA_BROKER_1:9092,KAFKA_BROKER_2:9092 taxi-trip-input tmp/checkpoint 1 c taxi-trip-collection 2181/solr

**Run Spark to Kudu:**

    spark-submit --class com.hadooparchitecturebook.taxi360.streaming.ingestion.kudu.SparkStreamingTaxiTripToKudu --master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 Taxi360.jar <KafkaBrokerList> <kafkaTopicList> <numberOfSeconds> <runLocal> <kuduMaster> <taxiEntityTableName> <kuduAppEventTable <checkPointFolder>

For example:

    spark-submit --class com.hadooparchitecturebook.taxi360.streaming.ingestion.kudu.SparkStreamingTaxiTripToKudu --master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 Taxi360.jar KAFKA_BROKER_1:9092,KAFKA_BROKER_2:9092 taxi-trip-input 1 c KUDU_MASTER ny_taxi_entity ny_taxi_trip tmp/checkpoint

**Run Spark to HBase:**

    spark-submit --class com.hadooparchitecturebook.taxi360.streaming.ingestion.hbase.SparkStreamingTaxiTripToHBase --master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 Taxi360.jar <KafkaBrokerList> <kafkaTopicList> <numberOfSeconds> <runLocal> <hbaseTable> <numOfSalts> <checkpointDir> <hbaseConfigFolder>

    spark-submit --class com.hadooparchitecturebook.taxi360.streaming.ingestion.hbase.SparkStreamingTaxiTripToHBase --master yarn --deploy-mode client --executor-memory 512MB --num-executors 2 --executor-cores 1 Taxi360.jar KAFKA_BROKER_1:9092,KAFKA_BROKER_2:9092 taxi-trip-input 1 c taxi-trip 6 /tmp/checkpoint /opt/cloudera/parcels/CDH/lib/hbase/conf/


Testing
-------

**Solr:**

Log into the Hue Search dashboard.

**Kudu:**

From the impala-shell or Hue Impala Query Editor:

    select * from ny_taxi_trip

**HBase REST Server:**

On the gateway node, start the REST server in a terminal:

    java -cp Taxi360.jar com.hadooparchitecturebook.taxi360.server.hbase.HBaseRestServer 4242 /opt/cloudera/parcels/CDH/lib/hbase/conf/ 6  taxi-trip

In another terminal, run some test requests:

    curl http://localhost:4242/rest/hello

    curl http://localhost:4242/rest/vender/0CMT/timeline

    curl http://localhost:4242/rest/vender/0CMT/timeline?startTime=0&endTime=1430944160000
    