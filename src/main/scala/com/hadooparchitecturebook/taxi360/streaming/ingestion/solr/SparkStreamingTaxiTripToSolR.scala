package com.hadooparchitecturebook.taxi360.streaming.ingestion.solr

import java.text.SimpleDateFormat
import java.util.Date

import com.hadooparchitecturebook.taxi360.common.SolrSupport
import com.hadooparchitecturebook.taxi360.model.{NyTaxiYellowTrip, NyTaxiYellowTripBuilder}
import kafka.serializer.StringDecoder
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.common.cloud.ZooKeeperException
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingTaxiTripToSolR {
  def main(args: Array[String]): Unit = {
    println("Java Version:" + System.getProperty("java.version"))
    println("Java Home:" + System.getProperties.getProperty("java.home"))

    val v: ZooKeeperException = null

    if (args.length == 0) {
      println("Args: <KafkaBrokerList> " +
        "<kafkaTopicList> " +
        "<checkpointDir>" +
        "<numberOfSeconds>" +
        "<runLocal>" +
        "<solrCollection>" +
        "<zkHost>")
      return
    }

    val kafkaBrokerList = args(0)
    val kafkaTopicList = args(1)
    val checkPointFolder = args(2)
    val numberOfSeconds = args(3).toInt
    val runLocal = args(4).equals("l")
    val solrCollection = args(5)
    val zkHost = args(6)

    println("kafkaBrokerList:" + kafkaBrokerList)
    println("kafkaTopicList:" + kafkaTopicList)
    println("numberOfSeconds:" + numberOfSeconds)
    println("runLocal:" + runLocal)
    println("solrCollection:" + solrCollection)
    println("zkHost:" + zkHost)

    val sc: SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConf = new SparkConf().setAppName("Spark Streaming Ingestion to SolR")
      new SparkContext(sparkConf)
    }
    val ssc = new StreamingContext(sc, Seconds(numberOfSeconds))

    val topicsSet = kafkaTopicList.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokerList)

    val messageStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val tripDStream = messageStream.map(r => {
      (r._1, r._2.split(","))
    }).filter(r => r._2.size > 3).map(r => {
      (r._1, NyTaxiYellowTripBuilder.build(r._2))
    })

    val solrDocumentDStream = tripDStream.map(convertToSolRDocuments)

    SolrSupport.indexDStreamOfDocs(zkHost,
      solrCollection,
      100,
      solrDocumentDStream)

    ssc.checkpoint(checkPointFolder)
    ssc.start()
    ssc.awaitTermination()
  }

  def convertToSolRDocuments(tripTuple: (String, NyTaxiYellowTrip)): SolrInputDocument = {
    val trip = tripTuple._2

    //2014-10-01T00:00:00Z
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")

    val doc: SolrInputDocument = new SolrInputDocument
    doc.addField("id", trip.vender_id + "," + trip.tpep_pickup_datetime)
    doc.addField("vender_id", trip.vender_id)
    doc.addField("tpep_pickup_datetime", dateFormat.format(new Date(trip.tpep_pickup_datetime)))
    doc.addField("tpep_dropoff_datetime", dateFormat.format(new Date(trip.tpep_dropoff_datetime)))
    doc.addField("passenger_count", trip.passenger_count)
    doc.addField("trip_distance", trip.trip_distance)
    doc.addField("pickup_longitude", trip.pickup_longitude)
    doc.addField("pickup_latitude", trip.pickup_latitude)
    doc.addField("dropoff_longitude", trip.dropoff_longitude)
    doc.addField("dropoff_latitude", trip.dropoff_latitude)
    doc.addField("payment_type", trip.payment_type)
    doc.addField("fare_amount", trip.fare_amount)
    doc.addField("extra", trip.extra)
    doc.addField("mta_tax", trip.mta_tax)
    doc.addField("improvement_surcharge", trip.improvement_surcharge)
    doc.addField("tip_amount", trip.tip_amount)
    doc.addField("tolls_amount", trip.tolls_amount)
    doc.addField("total_amount", trip.total_amount)

    doc
  }
}
