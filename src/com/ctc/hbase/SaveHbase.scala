package com.ctc.hbase

import com.ctc.util.AppUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{DataFrame, Row}



object SaveHbase{

  val FAMILY = Bytes.toBytes("count")
  val COL_ZJ = Bytes.toBytes("zj")
  val COL_BJ = Bytes.toBytes("bj")
  val TABLE = TableName.valueOf("billing:sms")



  def main(args: Array[String]): Unit = {
    val spark = AppUtils.createDefaultSession()
    val conf = spark.sparkContext.hadoopConfiguration
    conf.set("hbase.zookeeper.quorum", "node11:2181,node12:2181,node13:2181")
    conf.set("hbase.client.scanner.timeout.period",String.valueOf(5*60*1000))
    conf.set("zookeeper.znode.parent","/hbase-unsecure")


    def saveByHbaseContext(data:DataFrame): Unit ={
      val hContext  = new HBaseContext(spark.sparkContext,conf)
      hContext.bulkPut(data.rdd,TABLE,(row:Row) => {
        val nbr = row.getString(0)
        val zj = row.getLong(1).intValue()
        val bj = row.getLong(2).intValue()
        val rowKey = Bytes.toBytes(nbr)
        val put = new Put(rowKey).
          addImmutable(FAMILY, COL_ZJ, Bytes.toBytes(zj.byteValue())).
          addImmutable(FAMILY, COL_BJ, Bytes.toBytes(bj.byteValue()))
        put
      })
    }
    def saveByHbaseContextForeach(data:DataFrame): Unit ={
      val hContext  = new HBaseContext(spark.sparkContext,conf)
      val rdd = data.orderBy("ACC_NBR").rdd.filter(_.getString(0).matches("^1\\d{10}$")).map(row => {
        val nbr = row.getString(0)
        val zj = row.getLong(1).intValue()
        val bj = row.getLong(2).intValue()
        val rowKey = Bytes.toBytes(nbr)
        new Put(rowKey).
          addImmutable(FAMILY,COL_ZJ,Bytes.toBytes(zj)).
          addImmutable(FAMILY,COL_BJ,Bytes.toBytes(bj))

      })
      hContext.foreachPartition(
        rdd,
        (it:Iterator[Put],conn:Connection) =>{
          val mutate = conn.getBufferedMutator(TABLE)
          it.foreach(mutate.mutate(_))
        }
      )
    }

    def saveByLoadHFile(data:DataFrame): Unit = {
      conf.set(TableOutputFormat.OUTPUT_TABLE,TABLE.getNameAsString)
      val htable = new HTable(conf,TABLE)
      val job = Job.getInstance(conf)
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[KeyValue])
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
      val rdd = data.orderBy("ACC_NBR").rdd.filter(_.getString(0).matches("^1\\d{10}$")).flatMap(row => {
        val nbr = row.getString(0)
        val zj = row.getLong(1).intValue()
        val bj = row.getLong(2).intValue()
        val rowKey = Bytes.toBytes(nbr)
        val kv1 = new KeyValue(rowKey,FAMILY,COL_ZJ,Bytes.toBytes(zj))
        val kv2 = new KeyValue(rowKey,FAMILY,COL_BJ,Bytes.toBytes(bj))
        val k = new ImmutableBytesWritable(rowKey)
        Seq((k,kv1),(k,kv2))
      })

      rdd.saveAsNewAPIHadoopFile("/tmp/data1",classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],job.getConfiguration)
      val bulkLoader = new LoadIncrementalHFiles(conf)
      bulkLoader.doBulkLoad(new Path("/tmp/data1"),htable)

    }

    val PATH = "billing/sms/%s"
    val day= "20180815"

    val df = spark.read.parquet(PATH.format(day))
    val zj_cnt = df.groupBy("CallingNumber").
      count().toDF("ACC_NBR", "ZJ_CNT")
    val bj_cnt = df.groupBy("CalledNumber").
      count().toDF("ACC_NBR", "BJ_CNT")
    val data = zj_cnt.join(bj_cnt, Seq("ACC_NBR"), "outer").na.fill(0)

    saveByHbaseContextForeach(data)

  }


}