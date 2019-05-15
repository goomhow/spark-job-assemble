package com.ctc.location

import com.ctc.util.HDFSUtil
import com.ctc.util.INSTANTCE._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

class UserPosition(@transient spark:SparkSession)extends Serializable {
  /**
    * 将某天 或者 某几天的数据数据按时段 聚合
    * */
  def date_group(fname:String,struct:StructType): RDD[((String,String),Iterable[String])] = {
    spark.read.schema(struct).parquet(fname).rdd.filter(! _.anyNull).map(row =>{
      ((row.getString(2).take(10),row.getString(0)),row.getString(1))
    }).groupByKey()
  }
  /**
    * 计算某个号码通话次数最多的基站
    * */
  def most_cell(fname: String, strunct: StructType):RDD[Row]={
    date_group(fname,strunct).mapValues(_.toList.groupBy(s => s).mapValues(_.size).maxBy(_._1)._1).map(t => Row(t._1._1,t._1._2,t._2))
  }
  /**
    * 计算某个号码通话的基站及对应次数
    * */
  def user_cell_cnt(fname: String, strunct: StructType):RDD[Row] = {
    date_group(fname,strunct).flatMap(r => {
      val date_id = r._1._1
      val nbr = r._1._2
      r._2.toList.groupBy(s => s).mapValues(_.size).map(l=>Row(date_id,nbr,l._1,l._2))
    })
  }
  /**
    * 根据语音通话获取
    * (TIME,NBR,CELL) 或者 (TIME,NBR,CELL,CNT)
    */
    def get_voice_cell(month:String,resultStruct:StructType,op: (String,StructType) => RDD[Row]): DataFrame ={
      val path = s"user_position/voice/$month"
      if(HDFSUtil.exists(path)){
        spark.read.schema(resultStruct).parquet(path)
      }else{
        val v_mob_file = s"billing/mob/$month/ur_mob_voice_full_wh_$month"
        val v_ocs_file = s"billing/mob/$month/ur_ocs_voice_full_wh_$month"
        val v_mob_rdd = op(v_mob_file,voice_struct)
        val v_ocs_rdd = op(v_ocs_file,voice_struct)
        val df = spark.createDataFrame(v_mob_rdd.union(v_ocs_rdd),resultStruct)
        df.write.mode(SaveMode.Overwrite).parquet(path)
        df
      }
    }
  /**
    * 根据语音通话及4G流量信息获取
    * (TIME,NBR,CELL) 或者 (TIME,NBR,CELL,CNT)
    */
  def get_lte_voice_cell(month:String,resultStruct:StructType, op: (String,StructType) => RDD[Row]): DataFrame ={
    val path = s"user_position/lte_voice/$month"
    if(HDFSUtil.exists(path)){
      spark.read.schema(resultStruct).parquet(path)
    }else{
      val v_mob_file = s"billing/mob/$month/ur_mob_voice_full_wh_$month"
      val v_ocs_file = s"billing/mob/$month/ur_ocs_voice_full_wh_$month"
      val l_mob_file = s"billing/lte/$month/ur_mob_lte_wh_$month"
      val l_ocs_file = s"billing/lte/$month/ur_ocs_lte_wh_$month"
      val v_mob_rdd = op(v_mob_file,voice_struct)
      val v_ocs_rdd = op(v_ocs_file,voice_struct)
      val l_mob_rdd = op(l_mob_file,lte_struct)
      val l_ocs_rdd = op(l_ocs_file,lte_struct)
      val df = spark.createDataFrame(v_mob_rdd.union(v_ocs_rdd).union(l_mob_rdd).union(l_ocs_rdd),resultStruct)
      df.write.mode(SaveMode.Overwrite).parquet(path)
      df
    }
  }

  def getUserPositionByMax(month:String): DataFrame = {
    get_lte_voice_cell(month,USER_POSITION_STRUCT,most_cell)
  }

  def format_cell_pos(): Unit = {
    import org.apache.spark.sql.functions._
    val lte_pos = spark.read.parquet("position/tm_lte_cell")
    val voi_pos = spark.read.parquet("position/tm_voi_cell")
    val FLOAT_RE = "\\D*(\\d+\\.\\d+)\\D*".r
    def parseFloat = udf((data:String) => {
      if(data.matches(FLOAT_RE.regex)){
        val FLOAT_RE(f) = data
        f.toDouble
      }else{
        -0.1d
      }
    })
    val r_lte = lte_pos.select(lte_pos("CELL_ID"),parseFloat(lte_pos("LNG")).as("LNG"),parseFloat(lte_pos("LAT")).as("LAT"))
    val r_voi = voi_pos.select(voi_pos("CELL_ID"),parseFloat(voi_pos("LNG")).as("LNG"),parseFloat(voi_pos("LAT")).as("LAT"))
  }

}