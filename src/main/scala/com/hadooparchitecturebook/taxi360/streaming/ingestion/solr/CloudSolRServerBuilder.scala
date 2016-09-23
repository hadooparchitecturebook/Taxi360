package com.hadooparchitecturebook.taxi360.streaming.ingestion.solr

import org.apache.solr.client.solrj.impl.CloudSolrServer

object CloudSolRServerBuilder {
  val obj = new Object
  var cachedSolRServer:CloudSolrServer = null

  def build(zkHost:String): CloudSolrServer = {
    if (cachedSolRServer != null) {
      cachedSolRServer
    } else {
      obj.synchronized {
        if (cachedSolRServer == null) {
          cachedSolRServer = new CloudSolrServer(zkHost)
        }
      }
      cachedSolRServer
    }
  }
}
