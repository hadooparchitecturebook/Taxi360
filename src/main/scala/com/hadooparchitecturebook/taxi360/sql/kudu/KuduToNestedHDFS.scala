package com.hadooparchitecturebook.taxi360.sql.kudu

import com.hadooparchitecturebook.taxi360.model.NyTaxiYellowTripBuilder
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object KuduToNestedHDFS {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("Args: <runLocal> " +
        "<kuduMaster> " +
        "<kuduTaxiTripTableName> " +
        "<hdfsTaxiNestedTableName> ")
      return
    }

    val runLocal = args(0).equalsIgnoreCase("l")
    val kuduMaster = args(1)
    val kuduTaxiTripTableName = args(2)
    val hdfsTaxiNestedTableName = args(3)

    val sc: SparkContext = if (runLocal) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local", "TableStatsSinglePathMain", sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName("TableStatsSinglePathMain")
      new SparkContext(sparkConfig)
    }

    val hiveContext = new HiveContext(sc)

    val kuduOptions = Map(
      "kudu.table" -> kuduTaxiTripTableName,
      "kudu.master" -> kuduMaster)

    hiveContext.read.options(kuduOptions).format("org.apache.kudu.spark.kudu").load.
      registerTempTable("ny_taxi_trip_tmp")

    val kuduDataDf = hiveContext.sql("select * from ny_taxi_trip_tmp")

    val newNestedDf = kuduDataDf.map(r => {
      val pojo = NyTaxiYellowTripBuilder.build(r)
      (pojo.vender_id, pojo)
    }).groupByKey().map(grp => {
      Row(grp._1, grp._2.map(p => {
        Row(p.passenger_count,
          p.payment_type,
          p.total_amount,
          p.fare_amount)
      }))
    })

    hiveContext.sql("create table " + hdfsTaxiNestedTableName + "( " +
      " vender_id string," +
      " trip array<struct< " +
      "   passenger_count: INT," +
      "   payment_type: STRING, " +
      "   total_amount: DOUBLE, " +
      "   fare_amount: DOUBLE " +
      "  >>" +
      " ) stored as parquet")

    val emptyDf = hiveContext.sql("select * from " + hdfsTaxiNestedTableName + " limit 0")

    hiveContext.createDataFrame(newNestedDf, emptyDf.schema).registerTempTable("tmpNested")

    hiveContext.sql("insert into " + hdfsTaxiNestedTableName + " select * from tmpNested")

    sc.stop()
  }
}
