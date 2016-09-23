package com.hadooparchitecturebook.taxi360.etl.machinelearning.kudu

import com.hadooparchitecturebook.taxi360.model.{NyTaxiYellowTrip, NyTaxiYellowTripBuilder}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object MlLibOnKudu {
  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("Args: <runLocal> <kuduMaster> " +
        "<accountMartTableName> " +
        "<numOfCenters> " +
        "<numOfIterations> ")
      return
    }

    val runLocal = args(0).equalsIgnoreCase("l")
    val kuduMaster = args(1)
    val accountMartTableName = args(2)
    val numOfCenters = args(3).toInt
    val numOfIterations = args(4).toInt

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

    val sqlContext = new SQLContext(sc)

    val kuduOptions = Map(
      "kudu.table" -> accountMartTableName,
      "kudu.master" -> kuduMaster)

    sqlContext.read.options(kuduOptions).format("org.kududb.spark.kudu").load.
      registerTempTable("account_mart")

    //Vector
    val vectorRDD:RDD[Vector] = sqlContext.sql("select * from account_mart").map(r => {
      val taxiTrip = NyTaxiYellowTripBuilder.build(r)
      generateVectorOnly(taxiTrip)
    })

    println("--Running KMeans")
    val clusters = KMeans.train(vectorRDD, numOfCenters, numOfIterations)
    println(" > vector centers:")
    clusters.clusterCenters.foreach(v => println(" >> " + v))

    println("--Running corr")
    val correlMatrix: Matrix = Statistics.corr(vectorRDD, "pearson")
    println(" > corr: " + correlMatrix.toString)

    println("--Running colStats")
    val colStats = Statistics.colStats(vectorRDD)
    println(" > max: " + colStats.max)
    println(" > count: " + colStats.count)
    println(" > mean: " + colStats.mean)
    println(" > min: " + colStats.min)
    println(" > normL1: " + colStats.normL1)
    println(" > normL2: " + colStats.normL2)
    println(" > numNonZeros: " + colStats.numNonzeros)
    println(" > variance: " + colStats.variance)

    //Labeled Points
    /*
    val labeledPointRDD = sqlContext.sql("select * from customer_tran").map(r => {
      val event = AppEventBuilder.build(r)

      val authBool = if (event.eventType.equals(AppEventConst.)) 1 else 0

      new LabeledPoint(authBool, generateVectorForLabeledPoint(event))
    })

    val splits = labeledPointRDD.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    */

    //TODO
    //Sampleing by Key: http://spark.apache.org/docs/latest/mllib-statistics.html

  }

  def generateVectorOnly(taxiTrip: NyTaxiYellowTrip): Vector = {

    val array = Array(taxiTrip.tpep_dropoff_datetime.toDouble,
      taxiTrip.tpep_pickup_datetime.toDouble,
      taxiTrip.total_amount,
      taxiTrip.tolls_amount,
      taxiTrip.dropoff_latitude,
      taxiTrip.dropoff_longitude,
      taxiTrip.pickup_latitude,
      taxiTrip.pickup_longitude,
      taxiTrip.passenger_count)
    Vectors.dense(array)
  }
/*
  def generateVectorForLabeledPoint(accountMart: AccountMart): Vector = {
    val authValue: Double = if (tran.authFlag.equals(AppEventConst.AUTH_FLAG_APPROVED)) {
      1d
    } else {
      0d
    }

    val paymentTypeValue: Double = if (tran.paymentType.equals(AppEventConst.PAYMENT_TYPE_CREDIT)) {
      1d
    } else {
      0d
    }

    val cal = Calendar.getInstance()
    cal.setTime(new Date(tran.eventTimeStamp))

    val array = Array(tran.amount,
      authValue,
      paymentTypeValue,
      tran.eventTimeStamp.toDouble,
      cal.get(Calendar.DAY_OF_WEEK).toDouble,
      cal.get(Calendar.HOUR_OF_DAY).toDouble,
      tran.categoryType.hashCode.toDouble)
    Vectors.dense(array)
  }
  */
}
