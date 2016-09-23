package com.hadooparchitecturebook.taxi360.model

import javax.xml.bind.annotation.{XmlAccessType, XmlAccessorType, XmlRootElement}

import org.apache.kudu.client.RowResult
import org.apache.spark.sql.Row

@XmlRootElement(name = "CustomerMart")
@XmlAccessorType(XmlAccessType.FIELD)
class NyTaxiYellowEntity (val vender_id:String = "",
                          val total_trips:Int = 0,
                          val total_passengers:Int  = 0,
                          val total_distance_of_trips:Double  = 0,
                          val max_distance_of_trip:Double = 0,
                          val total_credit_card_fare_amount:Double = 0,
                          val total_create_card_extra:Double = 0,
                          val total_credit_card_mta_tax:Double = 0,
                          val total_credit_card_impr_surcharge:Double = 0,
                          val total_credit_card_tip_amount:Double = 0,
                          val total_credit_card_tolls_amount:Double = 0,
                          val total_credit_card_total_amount:Double = 0,
                          val total_cash_fare_amount:Double = 0,
                          val total_cash_extra:Double = 0,
                          val total_cash_mta_tax:Double = 0,
                          val total_cash_impr_surcharge:Double = 0,
                          val total_cash_tip_amount:Double = 0,
                          val total_cash_tolls_amount:Double = 0,
                          val total_cash_total_amount:Double = 0,
                          val total_credit_card_trips:Int = 0,
                          val total_cash_trips:Int = 0,
                          val total_no_charge_trips:Int = 0,
                          val total_dispute_trips:Int = 0,
                          val total_unknown_trips:Int = 0,
                          val total_voided_trips:Int = 0) extends Serializable{

  def + (trip:NyTaxiYellowTrip) : NyTaxiYellowEntity = {

    new NyTaxiYellowEntity(trip.vender_id,
      total_trips + 1,
      total_passengers + trip.passenger_count,
      total_dispute_trips + trip.trip_distance,
      Math.max(max_distance_of_trip, trip.trip_distance),
      total_credit_card_fare_amount + (if (trip.payment_type == "1") {trip.fare_amount} else {0.0}),
      total_create_card_extra + (if (trip.payment_type == "1") {trip.extra} else {0.0}),
      total_credit_card_mta_tax + (if (trip.payment_type == "1") {trip.mta_tax} else {0.0}),
      total_credit_card_impr_surcharge + (if (trip.payment_type == "1") {trip.improvement_surcharge} else {0.0}),
      total_credit_card_tip_amount + (if (trip.payment_type == "1") {trip.tip_amount} else {0.0}),
      total_credit_card_tolls_amount + (if (trip.payment_type == "1") {trip.tolls_amount} else {0.0}),
      total_credit_card_total_amount + (if (trip.payment_type == "1") {trip.total_amount} else {0.0}),
      total_cash_fare_amount + (if (trip.payment_type == "2") {trip.fare_amount} else {0.0}),
      total_cash_extra + (if (trip.payment_type == "2") {trip.extra} else {0.0}),
      total_cash_mta_tax + (if (trip.payment_type == "2") {trip.mta_tax} else {0.0}),
      total_cash_impr_surcharge + (if (trip.payment_type == "2") {trip.improvement_surcharge} else {0.0}),
      total_cash_tip_amount + (if (trip.payment_type == "2") {trip.tip_amount} else {0.0}),
      total_cash_tolls_amount + (if (trip.payment_type == "2") {trip.tolls_amount} else {0.0}),
      total_cash_total_amount + (if (trip.payment_type == "2") {trip.total_amount} else {0.0}),
      total_credit_card_trips + (if (trip.payment_type == "1") {1} else {0}),
      total_cash_trips + (if (trip.payment_type == "2") {1} else {0}),
      total_no_charge_trips + (if (trip.payment_type == "3") {1} else {0}),
      total_dispute_trips + (if (trip.payment_type == "4") {1} else {0}),
      total_unknown_trips + (if (trip.payment_type == "5") {1} else {0}),
      total_voided_trips + (if (trip.payment_type == "6") {1} else {0}))
  }

}

object NyTaxiYellowEntityBuilder {

  def build(rowResult:RowResult): NyTaxiYellowEntity = {
    new NyTaxiYellowEntity(rowResult.getString("vender_id"),
      rowResult.getInt(1),
      rowResult.getInt(2),
      rowResult.getDouble(3),
      rowResult.getDouble(4),//maxDist
      rowResult.getDouble(5),
      rowResult.getDouble(6),
      rowResult.getDouble(7),
      rowResult.getDouble(8),
      rowResult.getDouble(9),
      rowResult.getDouble(10),
      rowResult.getDouble(11),
      rowResult.getDouble(12),
      rowResult.getDouble(13),
      rowResult.getDouble(14),
      rowResult.getDouble(15),
      rowResult.getDouble(16),
      rowResult.getDouble(17),
      rowResult.getDouble(18),
      rowResult.getInt(19),
      rowResult.getInt(20),
      rowResult.getInt(21),
      rowResult.getInt(22),
      rowResult.getInt(23),
      rowResult.getInt(24))
  }

  def build(row:Row): NyTaxiYellowEntity = {
    new NyTaxiYellowEntity(row.getString(0),
      row.getInt(1),
      row.getInt(2),
      row.getDouble(3),
      row.getDouble(4),//maxDist
      row.getDouble(5),
      row.getDouble(6),
      row.getDouble(7),
      row.getDouble(8),
      row.getDouble(9),
      row.getDouble(10),
      row.getDouble(11),
      row.getDouble(12),
      row.getDouble(13),
      row.getDouble(14),
      row.getDouble(15),
      row.getDouble(16),
      row.getDouble(17),
      row.getDouble(18),
      row.getInt(19),
      row.getInt(20),
      row.getInt(21),
      row.getInt(22),
      row.getInt(23),
      row.getInt(24))
  }

  def build(trip:NyTaxiYellowTrip): NyTaxiYellowEntity = {
    new NyTaxiYellowEntity(trip.vender_id,
      1,
      trip.passenger_count,
      trip.trip_distance,
      trip.trip_distance,
      if (trip.payment_type == "1") {trip.fare_amount} else {0.0},
      if (trip.payment_type == "1") {trip.extra} else {0.0},
      if (trip.payment_type == "1") {trip.mta_tax} else {0.0},
      if (trip.payment_type == "1") {trip.improvement_surcharge} else {0.0},
      if (trip.payment_type == "1") {trip.tip_amount} else {0.0},
      if (trip.payment_type == "1") {trip.tolls_amount} else {0.0},
      if (trip.payment_type == "1") {trip.total_amount} else {0.0},
      if (trip.payment_type == "2") {trip.fare_amount} else {0.0},
      if (trip.payment_type == "2") {trip.extra} else {0.0},
      if (trip.payment_type == "2") {trip.mta_tax} else {0.0},
      if (trip.payment_type == "2") {trip.improvement_surcharge} else {0.0},
      if (trip.payment_type == "2") {trip.tip_amount} else {0.0},
      if (trip.payment_type == "2") {trip.tolls_amount} else {0.0},
      if (trip.payment_type == "2") {trip.total_amount} else {0.0},
      if (trip.payment_type == "1") {1} else {0},
      if (trip.payment_type == "2") {1} else {0},
      if (trip.payment_type == "3") {1} else {0},
      if (trip.payment_type == "4") {1} else {0},
      if (trip.payment_type == "5") {1} else {0},
      if (trip.payment_type == "6") {1} else {0})
  }

  //Blank
  //New
  //Modified
  //Untouched

  class NyTaxiYellowEntityStateWrapper (val state: String = "Blank",
                                        val entity: NyTaxiYellowEntity = new NyTaxiYellowEntity()) extends Serializable
}
