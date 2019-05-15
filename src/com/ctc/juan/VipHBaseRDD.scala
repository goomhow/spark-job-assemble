package com.ctc.juan

import java.util.Date

import org.apache.hadoop.hbase.{HBaseConfiguration, io}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.ctc.util.DateUtil._
import _root_.io.netty.util.CharsetUtil
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.DataFrame


class VipHBaseRDD(@transient sc:SparkContext) extends Serializable {
  val f_c = "(\\w+):(\\w+)".r
  val CALL = "vip:mldata_day"
  val CALLED = "vip:mldata_called_day"
  //val zookeeper = "133.0.6.85,133.0.6.86,133.0.6.87,133.0.6.88,133.0.6.89"
  val zookeeper = "node11:2181,node12:2181,node13:2181"
  val WORK_COLUMN = "work:top_cnt"
  val FREE_COLUMN = "free:top_cnt"
  val FULL_COLUMN = "full:top_cnt"
  val hConfig =  HBaseConfiguration.create()
  hConfig.set("hbase.zookeeper.quorum", zookeeper)
  hConfig.set("hbase.client.scanner.timeout.period",(5*60*1000).toString)
  hConfig.set("zookeeper.znode.parent","/hbase-unsecure")
  def hbase_rdd(tableName:String,start:String,stop:String,columns:String):RDD[(String,String)] ={
    val f_c(f,c) = columns
    val fb = f.getBytes()
    val cb = c.getBytes()
    val jobConf = new JobConf(hConfig)
    jobConf.set(TableInputFormat.INPUT_TABLE, tableName)
    jobConf.set(TableInputFormat.SCAN_ROW_START,start)
    jobConf.set(TableInputFormat.SCAN_ROW_STOP,stop)
    jobConf.set(TableInputFormat.SCAN_COLUMNS,columns)
    val rdd = sc.newAPIHadoopRDD(jobConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    rdd.mapPartitions(it => {
      it.map(t2=>{
        val k = new String(t2._1.copyBytes()).drop(9)
        val v = new String(t2._2.getValue(fb,cb))
        (k,v)
      })
    })
  }

  def save_rdd(rdd:RDD[(String,Map[String,String])],tableName:String): Unit = {
    val jobConf = new JobConf(hConfig)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val ts = new Date().getTime
    rdd.map{
      case (k,v) => {
        val put = new Put(k.getBytes(CharsetUtil.UTF_8),ts)
        v.foreach{
          case (fc,value) => {
            val f_c(family,qualifier) = fc
            put.addColumn(
              family.getBytes(CharsetUtil.UTF_8),
              qualifier.getBytes(CharsetUtil.UTF_8),
              value.getBytes(CharsetUtil.UTF_8)
            )
          }
        }
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsNewAPIHadoopDataset(
      hConfig
    )
  }

  def parse(rdd:RDD[(String,String)]): RDD[(String,String,Int)]= {
    rdd.flatMap(t=>{
      val z = t._1
      t._2.stripMargin.split(",").map(s => {
        val pair = s.stripMargin.split(":")
        val t = if(z>pair(0)) (pair(0),z) else (z,pair(0))
        (t,pair(1).takeRight(5).toInt)
      })
    }).groupByKey().map(r => {
      (r._1._1,r._1._2,r._2.sum)
    })
  }

  def month_rdd(days:List[List[String]],column:String): RDD[(String,String,Int)] = {
    val rdds = for(spice <- days)yield {
      val s = spice.take(1)(0)
      val e = spice.takeRight(1)(0)
      Seq(hbase_rdd(CALL,s,e,column),hbase_rdd(CALLED,s,e,column))
    }
    parse(rdds.flatMap(_.toList).reduce(_.union(_)))
  }

  def get_workday_df(month:String):RDD[((String,String),Int)]={
    val m1_days = smooth(get_work_days(month))
    month_rdd(m1_days,WORK_COLUMN).map(r => ((r._1,r._2),r._3))
  }

  def get_holiday_df(month:String):RDD[((String,String),Int)]={
    val m1_days = smooth(get_holiday(month))
    month_rdd(m1_days,WORK_COLUMN).map(r => ((r._1,r._2),r._3))
  }

  def get_free_time_df(month:String):RDD[((String,String),Int)] = {
    val m1_days = smooth(get_holiday(month))
    val m2_days = smooth(get_work_days(month))
    val rdd1 = month_rdd(m1_days,FULL_COLUMN).map(r => ((r._1,r._2),r._3))
    val rdd2 = month_rdd(m2_days,FREE_COLUMN).map(r => ((r._1,r._2),r._3))
    rdd1.union(rdd2)
  }

  def get_full_time_df(start_day:String,end_day:String):RDD[((String,String),Int)] = {
    val call = hbase_rdd(CALL,start_day,end_day,FULL_COLUMN)
    val called = hbase_rdd(CALLED,start_day,end_day,FULL_COLUMN)
    parse(call.union(called)).map(r => ((r._1,r._2),r._3))
  }
}
