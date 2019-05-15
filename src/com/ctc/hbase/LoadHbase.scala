package com.ctc.hbase


import com.ctc.util.AppUtils
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog

object LoadHbase {
  val FAMILY = Bytes.toBytes("count")
  val COL_ZJ = Bytes.toBytes("zj")
  val COL_BJ = Bytes.toBytes("bj")
  val TABLE = TableName.valueOf("billing:sms")

  val catalog =s"""{
                  |"table":{"namespace":"billing", "name":"sms"},
                  |"rowkey":"key",
                  |"columns":{
                  |"nbr":{"cf":"rowkey", "col":"key", "type":"string"},
                  |"zj":{"cf":"count", "col":"zj", "type":"int"},
                  |"bj":{"cf":"count", "col":"bj", "type":"int"}
                  |}
                  |}""".stripMargin

  case class SmsCount(
                     nbr:String="19999999999",
                     zj:Int=0,
                     bj:Int=0
                     )

  def main(args: Array[String]): Unit = {
    val spark = AppUtils.createDefaultSession()
    val conf = spark.sparkContext.hadoopConfiguration
    conf.set("hbase.zookeeper.quorum", "node11:2181,node12:2181,node13:2181")
    conf.set("hbase.client.scanner.timeout.period",String.valueOf(5*60*1000))
    conf.set("zookeeper.znode.parent","/hbase-unsecure")
  }
}