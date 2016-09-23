package com.hadooparchitecturebook.taxi360.server.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HBaseGlobalValues {
  var appEventTableName = "app-event"
  var numberOfSalts = 10000
  var connection:Connection = null

  def init(conf:Configuration, numberOfSalts:Int,
           appEventTableName:String): Unit = {
    connection = ConnectionFactory.createConnection(conf)
    this.numberOfSalts = numberOfSalts
    this.appEventTableName = appEventTableName
  }
}
