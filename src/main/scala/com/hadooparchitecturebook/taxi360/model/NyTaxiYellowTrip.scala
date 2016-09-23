package com.hadooparchitecturebook.taxi360.model

import java.text.SimpleDateFormat
import javax.xml.bind.annotation.{XmlAccessType, XmlAccessorType, XmlRootElement}

import org.apache.kudu.client.RowResult
import org.apache.spark.sql.Row

@XmlRootElement(name = "CustomerMart")
@XmlAccessorType(XmlAccessType.FIELD)
class NyTaxiYellowTrip (val vender_id:String,
                         val tpep_pickup_datetime:Long,
                         val tpep_dropoff_datetime:Long,
                         val passenger_count:Int,
                         val trip_distance:Double,
                         val pickup_longitude:Double,
                         val pickup_latitude:Double,
                         val rate_code_id:String,
                         val store_and_fwd_flag:String,
                         val dropoff_longitude:Double,
                         val dropoff_latitude:Double,
                         val payment_type:String,
                         val fare_amount:Double,
                         val extra:Double,
                         val mta_tax:Double,
                         val improvement_surcharge:Double,
                         val tip_amount:Double,
                         val tolls_amount:Double,
                         val total_amount:Double) extends Serializable{

}

object NyTaxiYellowTripBuilder {

  def build(row: RowResult): NyTaxiYellowTrip = {
    new NyTaxiYellowTrip(row.getString(0),
      row.getLong(1),
      row.getLong(2),
      row.getInt(3),
      row.getDouble(4),
      row.getDouble(5),
      row.getDouble(6),
      row.getString(7),
      row.getString(8),
      row.getDouble(9),
      row.getDouble(10),
      row.getString(11),
      row.getDouble(12),
      row.getDouble(13),
      row.getDouble(14),
      row.getDouble(15),
      row.getDouble(16),
      row.getDouble(17),
      row.getDouble(18)
    )
  }

  def build(row: Row): NyTaxiYellowTrip = {
    new NyTaxiYellowTrip(row.getString(0),
      row.getLong(1),
      row.getLong(2),
      row.getInt(3),
      row.getDouble(4),
      row.getDouble(5),
      row.getDouble(6),
      row.getString(7),
      row.getString(8),
      row.getDouble(9),
      row.getDouble(10),
      row.getString(11),
      row.getDouble(12),
      row.getDouble(13),
      row.getDouble(14),
      row.getDouble(15),
      row.getDouble(16),
      row.getDouble(17),
      row.getDouble(18)
    )
  }

  def build(cells:Array[String]): NyTaxiYellowTrip = {

    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    new NyTaxiYellowTrip(checkForEmptyNull(cells,0),
      format.parse(cells(1)).getTime,
      format.parse(cells(2)).getTime,
      checkForEmptyNull(cells,3).toInt,
      checkForEmptyNull(cells,4).toDouble,
      checkForEmptyNull(cells,5).toDouble,
      checkForEmptyNull(cells,6).toDouble,
      checkForEmptyNull(cells,7),
      checkForEmptyNull(cells,8),
      checkForEmptyNull(cells,9).toDouble,
      checkForEmptyNull(cells,10).toDouble,
      checkForEmptyNull(cells,11).toUpperCase(),
      checkForEmptyNull(cells,12).toDouble,
      checkForEmptyNull(cells,13).toDouble,
      checkForEmptyNull(cells,14).toDouble,
      checkForEmptyNull(cells,15).toDouble,
      checkForEmptyNull(cells,16).toDouble,
      checkForEmptyNull(cells,17).toDouble,
      checkForEmptyNull(cells,18).toDouble
    )
  }

  def checkForEmptyNull(s:Array[String], index:Int): String = {
    if (index >= s.size || s(index) == null || s(index).isEmpty) {
      "0"
    } else {
      s(index)
    }
  }
}
