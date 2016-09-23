package com.hadooparchitecturebook.taxi360.common

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

object
KafkaProducerUntil {
  def getNewProducer(brokerList:String,
                     acks:Int,
                     lingerMs:Int,
                     producerType:String,
                     batchSize:Int): KafkaProducer[String, String] = {
    val kafkaProps = new Properties
    kafkaProps.put("bootstrap.servers", brokerList)
    kafkaProps.put("metadata.broker.list", brokerList)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("acks", acks.toString)
    kafkaProps.put("retries", "3")
    kafkaProps.put("producer.type", producerType)
    kafkaProps.put("linger.ms", lingerMs.toString)
    kafkaProps.put("batch.size", batchSize.toString)

    println("brokerList:" + brokerList)
    println("acks:" + acks)
    println("lingerMs:" + lingerMs)
    println("batchSize:" + batchSize)
    println("producerType:" + producerType)
    println(kafkaProps)

    return new KafkaProducer[String,String](kafkaProps)
  }
}
