package com.hadooparchitecturebook.taxi360.server.kudu

import org.apache.kudu.client.KuduClient

object KuduGlobalValues {

  var appEventTableName = "app_event_kudu"
  var accountMartTableName = "account_mart_kudu"

  var kuduClient:KuduClient = null

  def init(kuduMaster:String,
           appEventTableName:String,
           accountMartTableName:String): Unit = {

    println("kuduMaster:" + kuduMaster)
    println("appEventTableName:" + appEventTableName)
    println("accountMartTableName:" + accountMartTableName)

    kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
    this.appEventTableName = appEventTableName
    this.accountMartTableName = accountMartTableName
  }

}
