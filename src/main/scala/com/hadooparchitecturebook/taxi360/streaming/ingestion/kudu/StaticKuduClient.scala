package com.hadooparchitecturebook.taxi360.streaming.ingestion.kudu

import org.apache.kudu.client.KuduClient

object StaticKuduClient {

  var kuduClient:KuduClient = null
  val LockObj = new Object

  def getKuduClient(kuduMaster:String): KuduClient = {
    LockObj.synchronized {
      if (kuduClient == null) {
        kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build()
      }
    }
    kuduClient
  }
}
