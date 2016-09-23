package com.hadooparchitecturebook.taxi360.server.kudu

import javax.ws.rs.core.MediaType
import javax.ws.rs.{QueryParam, _}

import com.hadooparchitecturebook.taxi360.model.{NyTaxiYellowEntity, NyTaxiYellowEntityBuilder, NyTaxiYellowTrip, NyTaxiYellowTripBuilder}
import org.apache.kudu.client.KuduPredicate

import scala.collection.mutable

@Path("rest")
class KuduServiceLayer {

  @GET
  @Path("hello")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def hello(): String = {
    "Hello World"
  }

  @GET
  @Path("vender/{venderId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getTaxiEntity (@PathParam("venderId") venderId:String): NyTaxiYellowEntity = {
    val kuduClient = KuduGlobalValues.kuduClient
    val custTable = KuduGlobalValues.kuduClient.openTable(KuduGlobalValues.accountMartTableName)

    val schema = custTable.getSchema
    val venderIdCol = schema.getColumn("vender_id")

    val scanner = kuduClient.newScannerBuilder(custTable).
      addPredicate(KuduPredicate.
        newComparisonPredicate(venderIdCol, KuduPredicate.ComparisonOp.EQUAL, venderId)).

      build()

    var taxiEntity:NyTaxiYellowEntity = null

    while (scanner.hasMoreRows) {
      val rows = scanner.nextRows()
      while (rows.hasNext) {
        val rowResult = rows.next()

        taxiEntity = NyTaxiYellowEntityBuilder.build(rowResult)
      }
    }

    taxiEntity
  }


  @GET
  @Path("vender/ts/{venderId}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getVenderTrips (@PathParam("venderId") venderId:String,
                          @QueryParam("startTime") startTime:Long = Long.MaxValue,
                          @QueryParam("endTime")  endTime:Long = Long.MinValue): Array[NyTaxiYellowTrip] = {
    val kuduClient = KuduGlobalValues.kuduClient
    val custTable = KuduGlobalValues.kuduClient.openTable(KuduGlobalValues.appEventTableName)

    val schema = custTable.getSchema
    val venderIdCol = schema.getColumn("venderId")
    val pickupDatetimeCol = schema.getColumn("tpep_pickup_datetime")

    val scanner = kuduClient.newScannerBuilder(custTable).
      addPredicate(KuduPredicate.
      newComparisonPredicate(venderIdCol, KuduPredicate.ComparisonOp.EQUAL, venderId)).
      addPredicate(KuduPredicate.
      newComparisonPredicate(pickupDatetimeCol, KuduPredicate.ComparisonOp.GREATER, startTime)).
      addPredicate(KuduPredicate.
      newComparisonPredicate(pickupDatetimeCol, KuduPredicate.ComparisonOp.LESS, endTime)).
      batchSizeBytes(1000000).build()


    val appEventList = new mutable.MutableList[NyTaxiYellowTrip]

    while (scanner.hasMoreRows) {
      println("-")
      val rows = scanner.nextRows()
      while (rows.hasNext) {
        println("--")
        val rowResult = rows.next()

        val appEvent = NyTaxiYellowTripBuilder.build(rowResult)

        appEventList += appEvent
      }
    }

    appEventList.toArray
  }
}
