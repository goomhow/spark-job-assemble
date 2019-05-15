package com.ctc.juan

import com.ctc.udaf.Vote
import com.ctc.util.{DateUtil, HDFSUtil}
import com.ctc.util.INSTANTCE._
import com.ctc.util.MapUtil._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

class SeedsGenerator(@transient spark: SparkSession)extends Serializable {
  @transient val sc = spark.sparkContext
  spark.udf.register("vote",new Vote)
  val TM_CORP_CELLS = spark.read.parquet("position/tm_corp_cells")
  TM_CORP_CELLS.cache()
  /**
    * 工作时间段通话
    * */
  def work_billing(fname:String,struct:StructType,wk:Set[String]): RDD[(String,Iterable[String])] = {
    spark.read.schema(struct).parquet(fname).rdd.filter(row =>{
      if(row.anyNull){
        false
      }else{
        val datetime = row.getString(2)
        if(datetime!=None&&datetime.size==14){
          val date =  datetime.take(8)
          val time = Integer.valueOf(datetime.takeRight(6))
          wk.contains(date)&&time>=90000&&time<=170000
        }else{
          false
        }
      }
    }).map(row =>{
      (row.getString(0),row.getString(1))
    }).groupByKey()
  }
  /**
    * 计算某个号码通话次数最多的基站
    * */
  def most_cell(fname: String, strunct: StructType, wk: Set[String]):RDD[Row]={
    work_billing(fname,strunct,wk).mapValues(_.toList.groupBy(s => s).mapValues(_.size).maxBy(_._1)._1).map(t => Row(t._1,t._2))
  }
  /**
    * 计算某个号码通话的基站及对应次数
    * */
  def user_cell_cnt(fname: String, strunct: StructType, wk: Set[String]):RDD[Row]={
    work_billing(fname,strunct,wk).flatMap(r => {
      val nbr = r._1
      r._2.toList.groupBy(s => s).mapValues(_.size).map(l=>Row(nbr,l._1,l._2))
    })
  }
  /**
    * 根据语音通话获取
    * (NBR,CELL) 或者 (NBR,CELL,CNT)
    */
    def get_voice_cell(month:String,region:String): DataFrame = {
      val path = s"position/tm_user_cell/month=$month/region=${region}"
      if(HDFSUtil.exists(path)){
        spark.read.schema(uc_struct).parquet(path)
      }else{
        val wk = DateUtil.get_work_days(month).toSet
        val v_mob_file = s"billing/mob/$month/ur_mob_voice_full_${region}_$month"
        val v_ocs_file = s"billing/mob/$month/ur_ocs_voice_full_${region}_$month"
        val v_mob_rdd = most_cell(v_mob_file,voice_struct,wk)
        val v_ocs_rdd = most_cell(v_ocs_file,voice_struct,wk)
        val df = spark.createDataFrame(v_mob_rdd.union(v_ocs_rdd),uc_struct)
        df.write.parquet(path)
        df
      }
    }


  def get_user_voted_cell(region_id:String): DataFrame ={
    val path=s"position/tm_vote_cell/region_id=$region_id"
    if(HDFSUtil.exists(path)){
      spark.read.parquet(path)
    }else{
      val months=Array("201708","201709","201710","201711","201712")
      val df = months.map(get_voice_cell(_,region_id)).reduce(_.unionAll(_)).groupBy("BILLING_NBR").agg(("CELL_ID","vote")).toDF("BILLING_NBR","CELL_ID")
      df.write.mode("overwrite").parquet(path)
      df
    }
  }

  def get_corp_cells(corps:Map[String,List[Double]],distance:Double=1000,maxCell:Int=10): DataFrame = {
    val fname="position/tm_corp_cells"
    val corp_cells = spark.read.parquet(fname)
    val contain_corp_df = corp_cells.filter(corp_cells("COMPANY").isin(corps.toArray.map(_._1):_*))
    val contain_corp_set = contain_corp_df.select("COMPANY").collect().map(_.getString(0)).toSet
    val not_contain_map = corps.filterKeys(!contain_corp_set.contains(_))
    if(not_contain_map.size>0){
      val cells = spark.read.parquet("position/tm_cell_info").collect()
      def get_cell_ids(pos:List[Double]): Array[String] ={
        cells.map(r => (r.getString(0),bd_distance(wgs84_to_bd09(r.getDouble(1),r.getDouble(2)),(pos(0),pos(1))))).
          filter(_._2<distance).sortWith(_._2<_._2).take(maxCell).map(_._1)
      }
      val rdd = sc.parallelize(not_contain_map.toSeq).map(r => {
        Row(r._1,r._2,get_cell_ids(r._2),r._2(0),r._2(1))
      })
      val df = spark.createDataFrame(rdd,company_cell_struct)
      df.write.mode("append").parquet(fname)
      df.unionAll(contain_corp_df)
    }else{
      contain_corp_df
    }
  }

  def save_corp_cells(table:String): Unit = {
    val fname="position/tm_corp_cells"
    val aDot = "^\\D*(\\d+\\.\\d+)\\D+(\\d+\\.\\d+)\\D*$".r
    val pos = spark.read.jdbc(anti_url,table,properties).select("CUST_NAME","BD_POS").where("CUST_NAME IS NOT NULL AND BD_POS IS NOT NULL").collect().map{
      case Row(name:String,pos:String) => (name,pos.stripMargin match {
        case aDot(lat,lng) => List(lat,lng).map(_.toDouble)
      })
    }
    val cells = spark.read.parquet("position/tm_cell_info").collect()
    def get_cell_ids(pos:List[Double]): Array[String] = {
      cells.map(r => (r.getString(1),bd_distance(wgs84_to_bd09(r.getDouble(2),r.getDouble(3)),(pos(0),pos(1))))).
        filter(_._2<1000.0).sortWith(_._2<_._2).take(10).map(_._1)
    }
    val rdd = sc.parallelize(pos.toSeq).map(r => {
      Row(r._1,get_cell_ids(r._2),r._2(0),r._2(1))
    })
    val df = spark.createDataFrame(rdd,company_cell_struct)
    df.write.mode("append").parquet(fname)
  }

  def generate_seeds(table:String,sheet_nbr:String,save_path:String="info/tc_tmp_hyxf_t"): Unit ={
    spark.read.jdbc(anti_url,table,properties).registerTempTable(table)
    spark.read.parquet("info/serv_t").registerTempTable("SERV_T")
    spark.read.parquet("info/cust_t").registerTempTable("CUST_T")
    val df = spark.sql(
      s"""
        |SELECT '$sheet_nbr' AS SHEET_NBR,C.AREA_NAME AS REGION_NAME,C.CUST_NAME,A.REGION_ID,A.ACC_NBR,A.SERVICE_TYPE,A.SERV_ID,A.AREA_CODE
        |FROM  SERV_T A,CUST_T B ,$table C
        |WHERE A.CUST_ID=B.CUST_ID
        |AND B.CUST_NAME = C.CUST_NAME
        |AND B.STATE='70A'
        |AND A.STATE='F0A'
        |AND A.SERV_STATE<>'F1R'
        |AND A.SERVICE_TYPE IN ('/s/t/fix','/s/t/mob')
      """.stripMargin)
    df.cache()
    df.write.mode("append").parquet(save_path)
    df.write.mode("append").jdbc(anti_url,"TC_TMP_HYXF_T",properties)
    df.unpersist()
  }

  /**
    * 基于基站群的工作地点计算
    * */
//  def get_user_corp_by_block(corp:String,region_id:String): DataFrame = {
//    val user_cell = get_user_voted_cell(region_id)
//    val corp_cell_rdd = get_corp_cells(corp_position).where(s"CORP='$corp'").select("COMPANY","CELL_ID").flatMap(r=>{
//      val com = r.getString(0)
//      for(cell<-r.getString(1).split(SEP)) yield Row(com,cell)
//    })
//    val corp_cell = sqlContext.createDataFrame(corp_cell_rdd,com_struct)
//    val df  = corp_cell.join(user_cell,"CELL_ID").groupBy("BILLING_NBR","COMPANY").sum("CELL_COUNT")
//    val rdd = df.map(r => (r.getString(0),(r.getString(1),r.getLong(2)))).groupByKey().mapValues(_.maxBy(_._2)._1)
//    sqlContext.createDataFrame(rdd.map(Row.fromTuple(_)),user_com_struct)
//  }
/**
  *
1001	武汉	027
1003	襄樊	0710
1004	黄冈	0713
1005	宜昌	0717
1006	孝感	0712
1007	鄂州	0711
1008	咸宁	0715
1009	十堰	0719
1010	荆门	0724
1011	黄石	0714
1012	随州	0722
1013	恩施	0718
1014	仙桃	0728
1015	天门	0728
1016	潜江	0728
1017	神农架	0719
1018	荆州	0716
  * */
  def getSeeds(region:String,corp: String, set: Set[String]):Set[String] = {
    val region_map = Map(
      "武汉"-> "wh",
      "襄樊"-> "xf",
      "襄阳"-> "xf",
      "黄冈" -> "hg",
      "宜昌" -> "yc",
      "孝感" -> "xg",
      "鄂州" -> "ez",
      "咸宁" -> "xn",
      "十堰" -> "sy",
      "荆门" -> "jm",
      "黄石" -> "hs",
      "随州" -> "sz",
      "恩施" -> "es",
      "仙桃" -> "xt",
      "天门" -> "tm",
      "潜江" -> "qj",
      "神农架" -> "snj",
      "林区" -> "snj",
      "荆州" -> "jz"
    )
    val user_cell = get_user_voted_cell(region_map(region))
    val set_cell = user_cell.filter(user_cell("BILLING_NBR").isin(set.toArray:_*))
    val r = TM_CORP_CELLS.where(TM_CORP_CELLS("COMPANY")===corp).select("CELL_ID").collect()
    if(r.size>0){
      val cells = r(0).getList(0).toArray().map(_.toString)
      set_cell.filter(set_cell("CELL_ID").isin(cells:_*)).select("BILLING_NBR").collect().map(_.getString(0)).toSet
    }else{
      Set[String]()
    }
  }
}
