package com.hadooparchitecturebook.taxi360.streaming.ingestion.hbase

import com.hadooparchitecturebook.taxi360.model.NyTaxiYellowTrip
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.util.Bytes

object TaxiTripHBaseHelper {

  val COLUMN_FAMILY = Bytes.toBytes("f")

  //taxiTrip Columns
  val DROP_OFF_DATETIME_COL = Bytes.toBytes("dof")
  val PASSENGER_COUNT_COL = Bytes.toBytes("pc")
  val TRIP_DISTANCE_COL = Bytes.toBytes("td")
  val PICKUP_LONGITUDE_COL = Bytes.toBytes("plg")
  val PICKUP_LATITUDE_COL = Bytes.toBytes("plt")
  val RATE_CODE_ID_COL = Bytes.toBytes("rc")
  val STORE_AND_FWD_FLAG_COL = Bytes.toBytes("sf")
  val DROPOFF_LONGITUDE_COL = Bytes.toBytes("dlg")
  val DROPOFF_LATITUDE_COL = Bytes.toBytes("dlt")
  val PAYMENT_TYPE_COL = Bytes.toBytes("pt")
  val FARE_AMOUNT_COL = Bytes.toBytes("fa")
  val EXTRA_COL = Bytes.toBytes("e")
  val MTA_TAX_COL = Bytes.toBytes("mt")
  val IMPROVEMENT_SURCHARGE_COL = Bytes.toBytes("is")
  val TIP_AMOUNT_COL = Bytes.toBytes("ta")
  val TOLLS_AMOUNT_COL = Bytes.toBytes("tla")
  val TOTAL_AMOUNT_COL = Bytes.toBytes("tta")


  def generateRowKey(taxiTrip: NyTaxiYellowTrip, numOfSalts:Int): Array[Byte] = {
    generateRowKey(taxiTrip.vender_id, taxiTrip.tpep_pickup_datetime, numOfSalts)
  }

  def generateRowKey(vender_id:String,
                     pickupDateTime:Long,
                     numOfSalts:Int): Array[Byte] = {
    val salt = StringUtils.leftPad(
      Math.abs(vender_id.hashCode % numOfSalts).toString, 4, "0")

    Bytes.toBytes(salt + ":" +
      vender_id + ":" +
      StringUtils.leftPad(pickupDateTime.toString, 11, "0"))
  }

  def generatePut(taxiTrip: NyTaxiYellowTrip, numOfSalts:Int): Put = {
    val put = new Put(generateRowKey(taxiTrip, numOfSalts))


    put.addColumn(COLUMN_FAMILY, DROP_OFF_DATETIME_COL,
      Bytes.toBytes(taxiTrip.tpep_dropoff_datetime.toString))
    put.addColumn(COLUMN_FAMILY, PASSENGER_COUNT_COL,
      Bytes.toBytes(taxiTrip.passenger_count.toString))
    put.addColumn(COLUMN_FAMILY, TRIP_DISTANCE_COL,
      Bytes.toBytes(taxiTrip.trip_distance.toString))
    put.addColumn(COLUMN_FAMILY, PICKUP_LONGITUDE_COL,
      Bytes.toBytes(taxiTrip.pickup_longitude.toString))
    put.addColumn(COLUMN_FAMILY, PICKUP_LATITUDE_COL,
      Bytes.toBytes(taxiTrip.pickup_latitude.toString))
    put.addColumn(COLUMN_FAMILY, RATE_CODE_ID_COL,
      Bytes.toBytes(taxiTrip.rate_code_id.toString))
    put.addColumn(COLUMN_FAMILY, STORE_AND_FWD_FLAG_COL,
      Bytes.toBytes(taxiTrip.store_and_fwd_flag.toString))
    put.addColumn(COLUMN_FAMILY, DROPOFF_LONGITUDE_COL,
      Bytes.toBytes(taxiTrip.dropoff_longitude.toString))
    put.addColumn(COLUMN_FAMILY, DROPOFF_LATITUDE_COL,
      Bytes.toBytes(taxiTrip.dropoff_latitude.toString))
    put.addColumn(COLUMN_FAMILY, PAYMENT_TYPE_COL,
      Bytes.toBytes(taxiTrip.payment_type.toString))
    put.addColumn(COLUMN_FAMILY, FARE_AMOUNT_COL,
      Bytes.toBytes(taxiTrip.fare_amount.toString))
    put.addColumn(COLUMN_FAMILY, EXTRA_COL,
      Bytes.toBytes(taxiTrip.extra.toString))
    put.addColumn(COLUMN_FAMILY, MTA_TAX_COL,
      Bytes.toBytes(taxiTrip.mta_tax.toString))
    put.addColumn(COLUMN_FAMILY, IMPROVEMENT_SURCHARGE_COL,
      Bytes.toBytes(taxiTrip.improvement_surcharge.toString))
    put.addColumn(COLUMN_FAMILY, TIP_AMOUNT_COL,
      Bytes.toBytes(taxiTrip.tip_amount.toString))
    put.addColumn(COLUMN_FAMILY, TOLLS_AMOUNT_COL,
      Bytes.toBytes(taxiTrip.tolls_amount.toString))
    put.addColumn(COLUMN_FAMILY, TOTAL_AMOUNT_COL,
      Bytes.toBytes(taxiTrip.total_amount.toString))

    put
  }

  def convertToTaxiTrip(result:Result): NyTaxiYellowTrip = {
    val keyParts = Bytes.toString(result.getRow).split(":")
    val vender_id = keyParts(1)
    val pickupDateTime = keyParts(2).toLong

    val drop_odd = getResultString(result, DROP_OFF_DATETIME_COL).toLong
    val passenger_count = getResultString(result, PASSENGER_COUNT_COL).toInt
    val trip_distance = getResultString(result, TRIP_DISTANCE_COL).toDouble
    val pickup_longitude = getResultString(result, PICKUP_LONGITUDE_COL).toDouble
    val pickup_latitude = getResultString(result, PICKUP_LATITUDE_COL).toDouble
    val rate_code_id = getResultString(result, RATE_CODE_ID_COL)
    val store_and_fwd_flag = getResultString(result, STORE_AND_FWD_FLAG_COL)
    val dropoff_longitude = getResultString(result, DROPOFF_LONGITUDE_COL).toDouble
    val dropoff_latitude = getResultString(result, DROPOFF_LATITUDE_COL).toDouble
    val payment_type = getResultString(result, PAYMENT_TYPE_COL)
    val fare_amount = getResultString(result, FARE_AMOUNT_COL).toDouble
    val extra = getResultString(result, EXTRA_COL).toDouble
    val mta_tax = getResultString(result, MTA_TAX_COL).toDouble
    val improvement_surcharge = getResultString(result, IMPROVEMENT_SURCHARGE_COL).toDouble
    val tip_amount = getResultString(result, TIP_AMOUNT_COL).toDouble
    val tolls_amount = getResultString(result, TOLLS_AMOUNT_COL).toDouble
    val total_amount = getResultString(result, TOTAL_AMOUNT_COL).toDouble

    new NyTaxiYellowTrip(vender_id,
      pickupDateTime,
      drop_odd,
      passenger_count,
      trip_distance,
      pickup_longitude,
      pickup_latitude,
      rate_code_id,
      store_and_fwd_flag,
      dropoff_longitude,
      dropoff_latitude,
      payment_type,
      fare_amount,
      extra,
      mta_tax,
      improvement_surcharge,
      tip_amount,
      tolls_amount,
      total_amount)
  }

  private def getResultString(result:Result, column:Array[Byte]): String = {
    val cell = result.getColumnLatestCell(COLUMN_FAMILY, column)
    Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }
}
