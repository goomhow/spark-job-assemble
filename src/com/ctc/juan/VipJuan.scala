package com.ctc.juan

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import com.ctc.util.INSTANTCE._
import com.ctc.util.DateUtil._
import com.ctc.juan.VipJuan._
import com.ctc.util.HDFSUtil

class VipJuan(@transient val spark: SparkSession) extends Serializable {
  @transient val sc = spark.sparkContext
  val hbase = new VipHBaseRDD(sc)
  // 基于交集的Juan
  def intersact(op:String=>RDD[((String,String),Int)])(m1:String,m2:String): RDD[((String,String),Int)] = {
    op(m1).union(op(m2)).groupByKey().filter(_._2.size>1).mapValues(_.sum)
  }

  def generateWorkdayJuan(m1:String,m2:String,m3:String,m4:String)(path:String=null): Unit = {
    def juan = intersact(hbase.get_workday_df)_
    val j1 = juan(m1,m3)
    val j2 = juan(m2,m4)
    val savePath = if(path!=null) path else WORKDAY_JUAN_PATH.format(m1,m4)
    spark.createDataFrame(
      j1.union(j2).groupByKey().mapPartitions(it => {
        it.map(r =>Row(r._1._1,r._1._2,r._2.sum))
      }),
      juan_struct
    ).write.mode(SaveMode.Overwrite).parquet(savePath)
  }

  def generateWorkdayJuanByEnd(m:String=null,path:String=null): Unit ={
    val args = get_prev_four_month(m)
    generateWorkdayJuan(args._1,args._2,args._3,args._4)(path)
  }

  def generateHolidayJuan(m1:String,m2:String,m3:String,m4:String)(path:String=null): Unit = {
    def juan = intersact(hbase.get_holiday_df)_
    val j1 = juan(m1,m3)
    val j2 = juan(m2,m4)
    val savePath = if(path!=null) path else HOLIDAY_JUAN_PATH.format(m1,m4)
    spark.createDataFrame(
      j1.union(j2).groupByKey().mapPartitions(it => {
        it.map(r =>Row(r._1._1,r._1._2,r._2.sum))
      }),
      juan_struct
    ).write.mode(SaveMode.Overwrite).parquet(savePath)
  }

  def generateHolidayJuanByEnd(m:String=null,path:String=null): Unit = {
    val args = get_prev_four_month(m)
    generateHolidayJuan(args._1,args._2,args._3,args._4)(path)
  }

  def generateFreeTimeJuan(m1:String,m2:String,m3:String,m4:String)(path:String=null): Unit = {
    def juan = intersact(hbase.get_free_time_df)_
    val j1 = juan(m1,m3)
    val j2 = juan(m2,m4)
    val savePath = if(path!=null) path else FREETIME_JUAN_PATH.format(m1,m4)
    spark.createDataFrame(
      j1.union(j2).groupByKey().mapPartitions(it => {
        it.map(r =>Row(r._1._1,r._1._2,r._2.sum))
      }),
      juan_struct
    ).write.mode(SaveMode.Overwrite).parquet(savePath)
  }

  def generateFreeTimeJuanByEnd(m:String=null,path:String=null): Unit = {
    val args = get_prev_four_month(m)
    generateHolidayJuan(args._1,args._2,args._3,args._4)(path)
  }

  //基于并集的Juan
  def generateUnionData(op: (String)=>(RDD[((String,String),Int)]))(path:String) = {
    val months = get_prev_six_month()
    val dfs = for(month <- months) yield {
      op(month).filter(r => r._1._1.matches(NBR_REG) && r._1._2.matches(NBR_REG)).groupByKey().mapValues(it => it.size*it.sum)
    }
    val data = dfs.reduce(_.union(_)).groupByKey().map{
      case ((x:String,y:String),z:Iterable[Int]) => Row(x,y,if(z.size==0) 0 else z.sum/z.size)
    }
    spark.createDataFrame(data,juan_struct).write.mode(SaveMode.Overwrite).parquet(path)
  }

  def getUnionWorkdayData(path:String=null): DataFrame = {
    val savePath = if(path==null) WORKDAY_UNION_PATH else path
    if(!HDFSUtil.exists(savePath))
      generateUnionData(hbase.get_workday_df)(savePath)
    spark.read.parquet(savePath)
  }

  def getUnionHolidayData(path:String=null): DataFrame ={
    val savePath = if(path==null) HOLIDAY_UNION_PATH else path
    if(!HDFSUtil.exists(savePath))
      generateUnionData(hbase.get_holiday_df)(savePath)
    spark.read.parquet(savePath)
  }

  def getUnionFreeTimeData(path:String=null): DataFrame = {
    val savePath = if(path==null) FREETIME_UNION_PATH else path
    if(!HDFSUtil.exists(savePath))
      generateUnionData(hbase.get_free_time_df)(savePath)
    spark.read.parquet(savePath)
  }

  def removeUnionDirs(): Unit = {
    HDFSUtil.removeDir(WORKDAY_UNION_PATH)
    HDFSUtil.removeDir(HOLIDAY_UNION_PATH)
    HDFSUtil.removeDir(FREETIME_UNION_PATH)
    HDFSUtil.removeDir(ENSEMBLE_UNION_PATH)
  }

}

object VipJuan{
  def apply(spark: SparkSession): VipJuan = new VipJuan(spark)
  val NBR_REG = "^0?\\d{6,12}$"
  val MOB_REG = "^0?0?(86)?1[3-9]\\d{9}$"

  val WORKDAY_JUAN_PATH = "workday_juan/%s_%s"
  val HOLIDAY_JUAN_PATH = "holiday_juan/%s_%s"
  val FREETIME_JUAN_PATH = "free_time_juan/%s_%s"

  val WORKDAY_UNION_PATH = "workday_union/"
  val HOLIDAY_UNION_PATH = "holiday_union/"
  val FREETIME_UNION_PATH = "free_time_union/"
  val ENSEMBLE_UNION_PATH = "ensemble_union/"
}