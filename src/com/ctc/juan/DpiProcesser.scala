package com.ctc.juan

import java.io.FileNotFoundException

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.WrappedArray
import com.ctc.util.INSTANTCE._
import com.ctc.util.JdbcUtil._
import com.ctc.util.HDFSUtil.exists


class DpiProcesser (@transient spark: SparkSession) extends Serializable {
  val ALL_PAHT = "dpi/all/%s"
  val MOB_PATH = "dpi/phone/%s"
  val ALL_COLUMNS = Array("AREA", "DATE_ID", "MDN", "TERMTYPE", "TERMBRAND", "TERMNO", "CLICK", "CLICK1")
  val MOB_COLUMNS = Array("AREA", "DATE_ID", "MDN", "ACCS_NBR", "TERMTYPE", "TERMBRAND", "TERMNO", "CLICK", "CLICK1")

  private def getPartitionInfo(td_table:String):List[(Int,Int)]={
    val (max,min) = getMaxMin(td_table)
    if(max/100==min/100){
      List((max,min))
    }else if(max/10000==min/10000){
      (min/100 to max/100).map(
        r => (r*100+1,r*100+31)
      ).toList
    }else{
      val l = (min/100 to ((min/10000)*100+12)).toList:::(((max/10000)*100+1) to max%100).toList
      l.map(r => (r*100+1,r*100+31))
    }
  }

  private def backup(td_table:String,max:Int,min:Int):Unit={
    val table = s"(SELECT * FROM $td_table WHERE Click1 <>'NULL' AND TermType='手机')A"
    val td_data = spark.read.jdbc(TD_URL,table,(min to max).map(month => s"Date_id = $month").toArray,TD_PROPERTY)
    td_data.repartition(500).write.mode("append").parquet(MOB_PATH.format(min.toString.take(6)))
  }

  private def backup(td_table:String):Unit={
    val p = getPartitionInfo(td_table)
    for((max,min)<-p){
      backup(td_table,max,min)
    }
  }

  def backupStandard(tabel:String,month:String,date_id:String="Date_id"): Unit ={
    val df = spark
      .read
      .jdbc(TD_URL,tabel,date_id,(month+"01").toInt,(month+"32").toInt,31,TD_PROPERTY)
      .repartition(100)
    val path = if (df.columns.length==9)MOB_PATH.format(month) else ALL_PAHT.format(month)
    val names = if (df.columns.length==9)MOB_COLUMNS else ALL_COLUMNS
    df.toDF(names:_*).write.mode("overwrite").parquet(path)
  }

  def get_dpi_data(month:String,is_all:Boolean=true):DataFrame={
    val path = if(is_all) ALL_PAHT.format(month) else MOB_PATH.format(month)
    val names = if(is_all) ALL_COLUMNS else MOB_COLUMNS
    spark.read.parquet(path).toDF(names:_*)
  }

  def menber_cnt(df:DataFrame):DataFrame={
    def myRound = udf((d:Double) => {
      val x = d.toInt
      if((d-x)>=0.3){
        x+1
      }else{
        x
      }
    })
    val x = df.groupBy("DATE_ID","MDN").agg(count(df("MDN")).as("MDN_CNT"))
      .groupBy("MDN").avg("MDN").withColumnRenamed("avg(MDN)","MDN_AVG")
      .filter("MDN_AVG >= 1.3 AND MDN_AVG<6.3")
    x.select(x("MDN"),myRound(x("MDN_AVG")).as("MENBER_CNT"))
  }

  def juan_mob_term(month:String):DataFrame={
    def vote = udf((list:WrappedArray[String])=>{
      list.aggregate(Map[String,Int]())((map:Map[String,Int],key:String)=>{
        val value = if(map.contains(key)) map(key)+1 else 1
        map.updated(key,value)
      },(map1:Map[String,Int],map2:Map[String,Int])=>{
        (map1.toList:::map2.toList).groupBy(_._1).mapValues(_.map(_._2).sum).toList.toMap[String,Int]
      }).toList.sortWith(_._2>_._2).takeRight(6)
    })
    val file = MOB_PATH.format(month)
    if(exists(file)){
      val df = spark.read.parquet(file)
      val cnt = menber_cnt(df)
      cnt.join(df,Seq("MDN"),"left")
        .groupBy("AREA","MDN","MENBER_CNT")
        .agg(collect_list("TERM").as("TERMS"))
    }else{
      throw new FileNotFoundException("该月份dpi数据不存在！")
    }
  }
}