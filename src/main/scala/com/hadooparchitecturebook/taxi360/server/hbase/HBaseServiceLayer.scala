package com.hadooparchitecturebook.taxi360.server.hbase

import javax.ws.rs._
import javax.ws.rs.core.MediaType

import com.hadooparchitecturebook.taxi360.model.NyTaxiYellowTrip
import com.hadooparchitecturebook.taxi360.streaming.ingestion.hbase.TaxiTripHBaseHelper
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

@Path("rest")
class HBaseServiceLayer {

  @GET
  @Path("hello")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def hello(): String = {
    "Hello World"
  }

  @GET
  @Path("vender/{venderId}/timeline")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getTripTimeLine (@PathParam("venderId") venderId:String,
                          @QueryParam("startTime") startTime:String = Long.MinValue.toString,
                          @QueryParam("endTime")  endTime:String = Long.MaxValue.toString): Array[NyTaxiYellowTrip] = {

    val table = HBaseGlobalValues.connection.getTable(TableName.valueOf(HBaseGlobalValues.appEventTableName))

    val st = if (startTime == null) {
      Long.MinValue.toString
    } else {
      startTime
    }
    val et = if (endTime == null) {
      Long.MaxValue.toString
    } else {
      endTime
    }

    val scan = new Scan()
    val startRowKey = TaxiTripHBaseHelper.generateRowKey(venderId, st.toLong, HBaseGlobalValues.numberOfSalts)
    println("startRowKey:" + Bytes.toString(startRowKey))
    scan.setStartRow(startRowKey)
    val endRowKey = TaxiTripHBaseHelper.generateRowKey(venderId, et.toLong, HBaseGlobalValues.numberOfSalts)
    println("endRowKey:" + Bytes.toString(endRowKey))
    scan.setStopRow(endRowKey)

    val scannerIt = table.getScanner(scan).iterator()

    val tripList = new mutable.MutableList[NyTaxiYellowTrip]

    while(scannerIt.hasNext) {
      val result = scannerIt.next()
      tripList += TaxiTripHBaseHelper.convertToTaxiTrip(result)
      println("Found a trip:" + TaxiTripHBaseHelper.convertToTaxiTrip(result))
    }

    println("tripList.size:" + tripList.size)

    tripList.toArray
  }

}
