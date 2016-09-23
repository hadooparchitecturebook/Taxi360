package com.hadooparchitecturebook.taxi360.common

import java.io.File
import java.util.Random

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object CsvKafkaPublisher {

  var counter = 0
  var salts = 0

  def main(args:Array[String]): Unit = {
    if (args.length == 0) {
      println("<brokerList> " +
        "<topicName> " +
        "<dataFolderOrFile> " +
        "<sleepPerRecord> " +
        "<acks> " +
        "<linger.ms> " +
        "<producer.type> " +
        "<batch.size> " +
        "<salts>")
      return
    }

    val kafkaBrokerList = args(0)
    val kafkaTopicName = args(1)
    val nyTaxiDataFolder = args(2)
    val sleepPerRecord = args(3).toInt
    val acks = args(4).toInt
    val lingerMs = args(5).toInt
    val producerType = args(6) //"async"
    val batchSize = args(7).toInt
    salts = args(8).toInt

    val kafkaProducer = KafkaProducerUntil.getNewProducer(kafkaBrokerList, acks, lingerMs, producerType, batchSize)

    println("--Input:" + nyTaxiDataFolder)

    val dataFolder = new File(nyTaxiDataFolder)
    if (dataFolder.isDirectory) {
      val files = dataFolder.listFiles().iterator
      files.foreach(f => {
        println("--Input:" + f)
        processFile(f, kafkaTopicName, kafkaProducer, sleepPerRecord)
      })
    } else {
      println("--Input:" + dataFolder)
      processFile(dataFolder, kafkaTopicName, kafkaProducer, sleepPerRecord)
    }
    println("---Done")
  }

  def processFile(file:File, kafkaTopicName:String,
                  kafkaProducer: KafkaProducer[String, String], sleepPerRecord:Int): Unit = {
    var counter = 0
    val r = new Random()

    println("-Starting Reading")
    Source.fromFile(file).getLines().foreach(l => {
      counter += 1
      if (counter % 10000 == 0) {
        println("{Sent:" + counter + "}")
      }
      if (counter % 100 == 0) {
        print(".")
      }
      Thread.sleep(sleepPerRecord)

      val saltedVender = r.nextInt(salts) + l

      if (counter > 2) {
        publishTaxiRecord(saltedVender, kafkaTopicName, kafkaProducer)
      }
    })
  }

  def publishTaxiRecord(line:String, kafkaTopicName:String, kafkaProducer: KafkaProducer[String, String]): Unit = {

    if (line.startsWith("vendor_name") || line.length < 10) {
      println("skip")
    } else {
      val message = new ProducerRecord[String, String](kafkaTopicName, line.hashCode.toString, line)
      kafkaProducer.send(message)
    }
  }


}
