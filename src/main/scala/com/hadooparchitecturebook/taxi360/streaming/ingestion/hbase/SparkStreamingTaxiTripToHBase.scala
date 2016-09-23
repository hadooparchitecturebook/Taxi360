package com.hadooparchitecturebook.taxi360.streaming.ingestion.hbase

import java.io.File

import com.hadooparchitecturebook.taxi360.model.NyTaxiYellowTripBuilder
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseDStreamFunctions._
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.solr.common.cloud.ZooKeeperException
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingTaxiTripToHBase {
  def main(args: Array[String]): Unit = {
    println("Java Version:" + System.getProperty("java.version"))
    println("Java Home:" + System.getProperties().getProperty("java.home"))

    val v:ZooKeeperException = null

    if (args.length == 0) {
      println("Args: <KafkaBrokerList> " +
        "<kafkaTopicList> " +
        "<numberOfSeconds>" +
        "<runLocal>" +
        "<hbaseTable>" +
        "<numOfSalts>" +
        "<checkpointDir>" +
        "<hbaseConfigFolder>")
      return
    }

    val kafkaBrokerList = args(0)
    val kafkaTopicList = args(1)
    val numberOfSeconds = args(2).toInt
    val runLocal = args(3).equals("l")
    val tableName = args(4)
    val numOfSalts = args(5).toInt
    val checkpointFolder = args(6)
    val hbaseConfigFolder = args(7)

    println("kafkaBrokerList:" + kafkaBrokerList)
    println("kafkaTopicList:" + kafkaTopicList)
    println("numberOfSeconds:" + numberOfSeconds)
    println("runLocal:" + runLocal)
    println("tableName:" + tableName)
    println("numOfSalts:" + numOfSalts)

    val sc:SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConf = new SparkConf().setAppName("Spark Streaming Ingestion to HBase")
      new SparkContext(sparkConf)
    }
    val ssc = new StreamingContext(sc, Seconds(numberOfSeconds))

    val topicsSet = kafkaTopicList.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokerList)

    val messageStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val conf = HBaseConfiguration.create()

    conf.addResource(new File(hbaseConfigFolder + "hbase-site.xml").toURI.toURL)

    val hbaseContext = new HBaseContext(sc, conf)

    val tripDStream = messageStream.map(r => {
      (r._1, r._2.split(","))
    }).filter(r => r._2.size > 3).map(r => {
      (r._1, NyTaxiYellowTripBuilder.build(r._2))
    })

    tripDStream.hbaseBulkPut(hbaseContext, TableName.valueOf(tableName), taxi => {
      TaxiTripHBaseHelper.generatePut(taxi._2, numOfSalts)
    })

    ssc.checkpoint(checkpointFolder)
    ssc.start()
    ssc.awaitTermination()
  }
}
