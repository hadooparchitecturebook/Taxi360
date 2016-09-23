package com.hadooparchitecturebook.taxi360.setup.hbase

import java.io.File

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.regionserver.{BloomType, ConstantSizeRegionSplitPolicy}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable


object CreateSaltedTable {
  def main(args:Array[String]): Unit = {

    if (args.length == 0) {
      println("<tableName> <columnFamily> <regionCount> <numOfSalts> <hbaseConfigFolder>")
    }
    val tableName = args(0)
    val columnFamilyName = args(1)
    val regionCount = args(2).toInt
    val numOfSalts = args(3).toInt
    val hbaseConfigFolder = args(4)

    val conf = HBaseConfiguration.create()

    conf.addResource(new File(hbaseConfigFolder + "hbase-site.xml").toURI.toURL)

    val connection = ConnectionFactory.createConnection(conf)

    val admin = connection.getAdmin

    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))

    val columnDescriptor = new HColumnDescriptor(columnFamilyName)

    columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY)
    columnDescriptor.setBlocksize(64 * 1024)
    columnDescriptor.setBloomFilterType(BloomType.ROW)

    tableDescriptor.addFamily(columnDescriptor)

    tableDescriptor.setMaxFileSize(Long.MaxValue)
    tableDescriptor.setRegionSplitPolicyClassName(classOf[ConstantSizeRegionSplitPolicy].getName)

    val splitKeys = new mutable.MutableList[Array[Byte]]
    for (i <- 0 to regionCount) {
      val regionSplitStr = StringUtils.leftPad((i*(numOfSalts/regionCount)).toString, 4, "0")
      splitKeys += Bytes.toBytes(regionSplitStr)
    }
    admin.createTable(tableDescriptor, splitKeys.toArray)
  }
}
