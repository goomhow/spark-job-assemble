package com.ctc.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppUtils {

  def createDefaultSession(appName:String = "default_app"): SparkSession ={
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.network.timeout","3600")
      .set("spark.executor.heartbeatInterval","3000")
    val spark =SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def main(args: Array[String]): Unit = {
    val spark = createDefaultSession("test")
    spark.range(0,100000).count()
  }

}
