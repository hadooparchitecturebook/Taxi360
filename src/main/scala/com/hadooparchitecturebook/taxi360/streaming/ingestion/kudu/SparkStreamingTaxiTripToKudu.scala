package com.hadooparchitecturebook.taxi360.streaming.ingestion.kudu

import com.hadooparchitecturebook.taxi360.model.NyTaxiYellowEntityBuilder.NyTaxiYellowEntityStateWrapper
import com.hadooparchitecturebook.taxi360.model.{NyTaxiYellowTrip, NyTaxiYellowTripBuilder}
import kafka.serializer.StringDecoder
import org.apache.kudu.client.{KuduClient, Operation}
import org.apache.kudu.client.SessionConfiguration.FlushMode
import org.apache.solr.common.cloud.ZooKeeperException
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingTaxiTripToKudu {

  val paymentTypeMap = Map("1" -> "Credit card",
    "2" -> "Cash",
    "3" -> "No charge",
    "4" -> "Dispute",
    "5" -> "Unknown",
    "6" -> "Voided trip")

  val rateCodeId = Map("1" -> "Standard rate",
    "2" -> "JFK",
    "3" -> "Newark",
    "4" -> "Nassau or Westchester",
    "5" -> "Negotiated fare",
    "6" -> "Group ride")


  def main(args: Array[String]): Unit = {
    println("Java Version:" + System.getProperty("java.version"))
    println("Java Home:" + System.getProperties().getProperty("java.home"))

    val v: ZooKeeperException = null

    if (args.length == 0) {
      println("Args: <KafkaBrokerList> " +
        "<kafkaTopicList> " +
        "<numberOfSeconds>" +
        "<runLocal>" +
        "<kuduMaster>" +
        "<kdudTaxiTripEntityTableName>",
        "<kuduTaxiTripTable",
        "<checkPointFolder>")
      return
    }

    val kafkaBrokerList = args(0)
    val kafkaTopicList = args(1)
    val numberOfSeconds = args(2).toInt
    val runLocal = args(3).equals("l")
    val kuduMaster = args(4)
    val kdudTaxiTripEntityTableName = args(5)
    val kuduTaxiTripTable = args(6)
    val checkPointFolder = args(7)

    println("kafkaBrokerList:" + kafkaBrokerList)
    println("kafkaTopicList:" + kafkaTopicList)
    println("numberOfSeconds:" + numberOfSeconds)
    println("runLocal:" + runLocal)
    println("kuduMaster:" + kuduMaster)
    println("taxiEntityTableName:" + kdudTaxiTripEntityTableName)
    println("taxiTripTableName:" + kuduTaxiTripTable)
    println("checkPointFolder:" + checkPointFolder)

    val sc: SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConf = new SparkConf().setAppName("Spark Streaming Ingestion to Kudu")
      new SparkContext(sparkConf)
    }
    val ssc = new StreamingContext(sc, Seconds(numberOfSeconds))

    val topicsSet = kafkaTopicList.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrokerList)

    val kuduMasterBc = sc.broadcast(kuduMaster)

    val messageStream = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val tripDStream = messageStream.map(r => {
      (r._1, r._2.split(","))
    }).filter(r => r._2.size > 3).map(r => {
      (r._1, NyTaxiYellowTripBuilder.build(r._2))
    })

    tripDStream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        sendTripToKudu(kuduTaxiTripTable, it, StaticKuduClient.getKuduClient(kuduMasterBc.value))
      })
    })

    tripDStream.updateStateByKey[NyTaxiYellowEntityStateWrapper](
      (newTrips: Seq[NyTaxiYellowTrip],
       taxiEntityWrapperOp: Option[NyTaxiYellowEntityStateWrapper]) => {

        val taxiEntityWrapper = taxiEntityWrapperOp.getOrElse(new NyTaxiYellowEntityStateWrapper)
        var newTaxiEntity = taxiEntityWrapper.entity
        newTrips.foreach(trip => {
          newTaxiEntity = newTaxiEntity + trip
        })

        val newState = if (taxiEntityWrapper.state.equals("Blank")) {
          "New"
        } else if (newTrips.length > 0) {
          "Modified"
        } else {
          "Untouched"
        }

        Option(new NyTaxiYellowEntityStateWrapper(newState, newTaxiEntity))
      }).foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        sendEntityToKudu(kdudTaxiTripEntityTableName, it, StaticKuduClient.getKuduClient(kuduMasterBc.value))
      })
    })



    println("--Starting Spark Streaming")
    ssc.checkpoint(checkPointFolder)
    ssc.start()
    ssc.awaitTermination()
  }

    def sendEntityToKudu(taxiEntityTableName: String, it: Iterator[(String, NyTaxiYellowEntityStateWrapper)], kuduClient: KuduClient): Unit = {
      val table = kuduClient.openTable(taxiEntityTableName)
      val session = kuduClient.newSession()
      session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

      it.foreach(r => {
        val state = r._2.state
        val entity = r._2.entity

        val operation: Operation = if (state.equals("New")) {
          table.newInsert()
        } else if (state.equals("Modified")) {
          table.newUpdate()
        } else {
          null
        }

        if (operation != null) {
          val row = operation.getRow()

          row.addString("vender_id", entity.vender_id)
          row.addInt("total_trips", entity.total_trips)
          row.addInt("total_passengers", entity.total_passengers)
          row.addDouble("total_distance_of_trips", entity.total_distance_of_trips)
          row.addDouble("max_distance_of_trip", entity.max_distance_of_trip)
          row.addDouble("total_credit_card_fare_amount", entity.total_credit_card_fare_amount)
          row.addDouble("total_create_card_extra", entity.total_create_card_extra)
          row.addDouble("total_credit_card_mta_tax", entity.total_credit_card_mta_tax)
          row.addDouble("total_credit_card_impr_surcharge", entity.total_credit_card_impr_surcharge)
          row.addDouble("total_credit_card_tip_amount", entity.total_credit_card_tip_amount)
          row.addDouble("total_credit_card_tolls_amount", entity.total_credit_card_tolls_amount)
          row.addDouble("total_credit_card_total_amount", entity.total_credit_card_total_amount)
          row.addDouble("total_cash_fare_amount", entity.total_cash_fare_amount)
          row.addDouble("total_cash_extra", entity.total_cash_extra)
          row.addDouble("total_cash_mta_tax", entity.total_cash_mta_tax)
          row.addDouble("total_cash_impr_surcharge", entity.total_cash_impr_surcharge)
          row.addDouble("total_cash_tip_amount", entity.total_cash_tip_amount)
          row.addDouble("total_cash_tolls_amount", entity.total_cash_tolls_amount)
          row.addDouble("total_cash_total_amount", entity.total_cash_total_amount)
          row.addInt("total_credit_card_trips", entity.total_credit_card_trips)
          row.addInt("total_cash_trips", entity.total_cash_trips)
          row.addInt("total_no_charge_trips", entity.total_no_charge_trips)
          row.addInt("total_dispute_trips", entity.total_dispute_trips)
          row.addInt("total_unknown_trips", entity.total_unknown_trips)
          row.addInt("total_voided_trips", entity.total_voided_trips)

          session.apply(operation)
        }

      })
      session.flush()
      session.close()
    }

    def sendTripToKudu(taxiTripTableName: String, it: Iterator[(String, NyTaxiYellowTrip)], kuduClient: KuduClient): Unit = {
      val table = kuduClient.openTable(taxiTripTableName)
      val session = kuduClient.newSession()
      session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND)

      it.foreach(r => {
        val trip = r._2
        val operation = table.newInsert()
        val row = operation.getRow()

        row.addString("vender_id", trip.vender_id)
        row.addLong("tpep_pickup_datetime", trip.tpep_pickup_datetime)
        row.addLong("tpep_dropoff_datetime", trip.tpep_dropoff_datetime)
        row.addInt("passenger_count", trip.passenger_count)
        row.addDouble("trip_distance", trip.trip_distance)
        row.addDouble("pickup_longitude", trip.pickup_longitude)
        row.addDouble("pickup_latitude", trip.pickup_latitude)
        row.addString("rate_code_id", rateCodeId.getOrElse(trip.rate_code_id, "N/A"))
        row.addString("store_and_fwd_flag", trip.store_and_fwd_flag)
        row.addDouble("dropoff_longitude", trip.dropoff_longitude)
        row.addDouble("dropoff_latitude", trip.dropoff_latitude)
        row.addString("payment_type", trip.payment_type)
        row.addDouble("fare_amount", trip.fare_amount)
        row.addDouble("extra", trip.extra)
        row.addDouble("mta_tax", trip.mta_tax)
        row.addDouble("improvement_surcharge", trip.improvement_surcharge)
        row.addDouble("tip_amount", trip.tip_amount)
        row.addDouble("tolls_amount", trip.tolls_amount)
        row.addDouble("total_amount", trip.total_amount)

        try {
          session.apply(operation)
        } catch {
          case e: Exception => {
            //nothing
          }
        }
      })
      try {
        session.flush()
        session.close()
      } catch {
        case e: Exception => {
          //nothing
        }
      }
    }

}
