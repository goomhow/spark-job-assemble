package com.ctc.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, filter}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.spark.HBaseContext

import scala.collection.JavaConversions.mapAsScalaMap

object HBaseUtil {
  val conf = HBaseConfiguration.create()
  conf.addResource("/etc/hbase/conf/hbase-site.xml")
  val zookeeper = "133.0.6.85,133.0.6.86,133.0.6.87,133.0.6.88,133.0.6.89"
  //val zookeeper = "133.0.6.95,133.0.6.96,133.0.6.97"
  val f_c = "(\\w+):(\\w+)".r
  //full:base_billing
  def hbase_rdd(sc:SparkContext,tableName:String)(columns:String)(start:String, stop:String):RDD[(String,String)] = {
    val hConfig =  HBaseConfiguration.create()
    hConfig.set("hbase.zookeeper.quorum", zookeeper)
    hConfig.set(TableInputFormat.INPUT_TABLE, tableName)
    hConfig.set(TableInputFormat.SCAN_ROW_START,start)
    hConfig.set(TableInputFormat.SCAN_ROW_STOP,stop)
    hConfig.set(TableInputFormat.SCAN_COLUMNS,columns)
    sc.newAPIHadoopRDD(hConfig, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).mapPartitions(it => {it.map(t2=>{
      val k = Bytes.toString(t2._1.get()).drop(9)
      val f_c(f,c)=columns
      val v = Bytes.toString(t2._2.getValue(Bytes.toBytes(f),Bytes.toBytes(c)))
      (k,v)})
    })
  }

  def scan(sc:SparkContext)(table:String,start:String,end:String): Unit = {
    val hbaseContext = new HBaseContext(sc,conf)
    val filters = new FilterList(FilterList.Operator.MUST_PASS_ALL)
    val lessThan1000 = new SingleColumnValueFilter("free".getBytes(),"opp_cnt".getBytes(),CompareOp.LESS_OR_EQUAL,new filter.LongComparator(1000))
    val greaterThan10 = new SingleColumnValueFilter("free".getBytes(),"opp_cnt".getBytes(),CompareOp.GREATER_OR_EQUAL,new filter.LongComparator(10))
    filters.addFilter(lessThan1000)
    filters.addFilter(greaterThan10)
    val scan = new Scan()
    scan.setStartRow(start.getBytes())
    scan.setStopRow(end.getBytes())
    scan.setFilter(filters)
    val rdd = hbaseContext.hbaseRDD(TableName.valueOf(table),scan,(row) =>{
      val rowKey = row._1
      val result = row._2
      val map = mapAsScalaMap(result.getFamilyMap("free".getBytes())).map{
        case (a,b) => (Bytes.toString(a),Bytes.toString(b))
      }
      val Array(day,nbr) = Bytes.toString(rowKey.get()).split("_")
      (day,nbr,map)
    })
  }
}
