package com.ctc.juan

import com.ctc.util.DateUtil._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


class VipOldHBaseRDD(@transient sc: SparkContext) extends Serializable {

  val CALL = "vip:mldata_day"
  val CALLED = "vip:mldata_called_day"
  val zookeeper = "133.0.6.85,133.0.6.86,133.0.6.87,133.0.6.88,133.0.6.89"
  val WORK_COLUMN = "work:top_cnt"
  val FREE_COLUMN = "free:top_cnt"
  val FULL_COLUMN = "full:top_cnt"

  def hbase_rdd(tableName: String, start: String, stop: String, columns: String): RDD[(String, String)] = {
    val f_c = "(\\w+):(\\w+)".r
    val f_c(f, c) = columns
    val fb = f.getBytes()
    val cb = c.getBytes()
    val hConfig = HBaseConfiguration.create()
    hConfig.set("hbase.zookeeper.quorum", zookeeper)
    hConfig.set("hbase.master", "133.0.6.85:60000")
    hConfig.set("hbase.client.scanner.timeout.period", (5 * 60 * 1000).toString)
    hConfig.set(TableInputFormat.INPUT_TABLE, tableName)
    hConfig.set(TableInputFormat.SCAN_ROW_START, start)
    hConfig.set(TableInputFormat.SCAN_ROW_STOP, stop)
    hConfig.set(TableInputFormat.SCAN_COLUMNS, columns)
    val rdd = sc.newAPIHadoopRDD(
      hConfig,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    rdd.mapPartitions(it => {
      it.map(t2 => {
        val k = new String(t2._1.copyBytes()).drop(9)
        val v = new String(t2._2.getValue(fb, cb))
        (k, v)
      })
    })
  }

  def save_rdd(sc: SparkContext, rdd: RDD[(String, String)], tableName: String, column: String): Unit = {
    val hConfig = HBaseConfiguration.create()
    hConfig.set("hbase.zookeeper.quorum", zookeeper)
    hConfig.set("hbase.client.scanner.timeout.period", (5 * 60 * 1000).toString)
    hConfig.set("zookeeper.znode.parent", "/hbase-unsecure")
    val jobConf = new JobConf(hConfig)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    rdd.saveAsNewAPIHadoopDataset(
      hConfig
    )
  }


  def parse(rdd: RDD[(String, String)]): RDD[(String, String, Int)] = {
    rdd.flatMap(t => {
      val z = t._1
      t._2.stripMargin.split(",").map(s => {
        val pair = s.stripMargin.split(":")
        val t = if (z > pair(0)) (pair(0), z) else (z, pair(0))
        (t, pair(1).takeRight(5).toInt)
      })
    }).groupByKey().map(r => {
      (r._1._1, r._1._2, r._2.sum)
    })
  }

  def month_rdd(days: List[List[String]], column: String): RDD[(String, String, Int)] = {
    val rdds = for (spice <- days) yield {
      val s = spice.take(1)(0)
      val e = spice.takeRight(1)(0)
      Seq(hbase_rdd(CALL, s, e, column), hbase_rdd(CALLED, s, e, column))
    }
    parse(rdds.flatMap(_.toList).reduce(_.union(_)))
  }

  def get_workday_df(month: String): RDD[((String, String), Int)] = {
    val m1_days = smooth(get_work_days(month))
    month_rdd(m1_days, WORK_COLUMN).map(r => ((r._1, r._2), r._3))
  }

  def get_holiday_df(month: String): RDD[((String, String), Int)] = {
    val m1_days = smooth(get_holiday(month))
    month_rdd(m1_days, WORK_COLUMN).map(r => ((r._1, r._2), r._3))
  }

  def get_free_time_df(month: String): RDD[((String, String), Int)] = {
    val m1_days = smooth(get_holiday(month))
    val m2_days = smooth(get_work_days(month))
    val rdd1 = month_rdd(m1_days, FULL_COLUMN).map(r => ((r._1, r._2), r._3))
    val rdd2 = month_rdd(m2_days, FREE_COLUMN).map(r => ((r._1, r._2), r._3))
    rdd1.union(rdd2)
  }

}
