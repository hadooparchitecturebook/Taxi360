package com.hadooparchitecturebook.taxi360.sql.kudu

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object KuduToHDFS {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("Args: <runLocal> <kuduMaster> " +
        "<kuduTaxiTripTableName> " +
        "<hdfsTaxiTripTableName> " +
        "<numOfCenters> " +
        "<numOfIterations> ")
      return
    }

    val runLocal = args(0).equalsIgnoreCase("l")
    val kuduMaster = args(1)
    val kuduTaxiTripTableName = args(2)
    val hdfsTaxiTripTableName = args(3)

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

    hiveContext.read.options(kuduOptions).format("org.kududb.spark.kudu").load.
      registerTempTable("kuduTaxiTripTableName")

    hiveContext.sql("CREATE TABLE " + hdfsTaxiTripTableName + " " +
      " AS SELECT * FROM kuduTaxiTripTableName " +
      " STORED AS PARQUET")

    sc.stop()
  }
}
