package com.ctc.backup

import org.apache.spark.sql.{DataFrame, SparkSession}

class OracleQuery(@transient spark:SparkSession) {

  def getDanC(partition_id_region:Any): DataFrame ={
    val serv_t = spark.read.parquet("info/serv_t")
    val product_offer_detail_t = spark.read.parquet("info/product_offer_detail_t")
    serv_t.createOrReplaceTempView("serv_t")
    product_offer_detail_t.createOrReplaceTempView("product_offer_detail_t")
    val sql = s"""select
                |a.acc_nbr,a.serv_id  from serv_t a
                |where a.state='F0A' and a.service_type='/s/t/mob' and partition_id_region=${partition_id_region}
                |and a.product_offer_id in (
                |select b.offer_id  from product_offer_detail_t  b
                |group by b.offer_id
                |having count(*)=1)"""
    val result = spark.sql(sql)
    result
  }

  def getAllDanC(): DataFrame ={
    val serv_t = spark.read.parquet("info/serv_t")
    val product_offer_detail_t = spark.read.parquet("info/product_offer_detail_t")
    serv_t.createOrReplaceTempView("serv_t")
    product_offer_detail_t.createOrReplaceTempView("product_offer_detail_t")
    val sql = s"""select
                 |a.acc_nbr,a.serv_id  from serv_t a
                 |where a.state='F0A' and a.service_type='/s/t/mob'
                 |and a.product_offer_id in (
                 |select b.offer_id  from product_offer_detail_t  b
                 |group by b.offer_id
                 |having count(*)=1
                 |)"""
    val result = spark.sql(sql)
    result
  }

  def getContact(partition_id_region:Any): DataFrame ={
    val sql =s"""select a.serv_id,a.acc_nbr,b.telephone,b.mobile from serv_t a
               left join cust_contact_info_t b on a.cust_id =b.cust_id
               where a.state='F0A'
               and a.serv_state<>	'F1R'
               and a.service_type = '/s/i/kd'
               and a.partition_id_region=${partition_id_region}
               and b.state='70A'"""
    val serv_t = spark.read.parquet("info/serv_t")
    val cust_contact_info_t = spark.read.parquet("info/cust_contact_info_t")
    serv_t.createOrReplaceTempView("serv_t")
    cust_contact_info_t.createOrReplaceTempView("cust_contact_info_t")
    val result = spark.sql(sql)
    result
  }

}
