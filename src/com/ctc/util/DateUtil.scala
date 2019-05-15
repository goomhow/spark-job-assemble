package com.ctc.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import scala.io.Source
import scala.util.parsing.json.JSON

object DateUtil extends Serializable {
  val yyyyMM = new SimpleDateFormat("yyyyMM")
  val yyyyMMdd = new SimpleDateFormat("yyyyMMdd")
  val yyyyMMddHH = new SimpleDateFormat("yyyyMMddHH")
  val SECONDS_FMT = new SimpleDateFormat("yyyyMMddHHmmss")

  def get_work_days(month:String):List[String]={
    val json = Source.fromURL(s"http://133.0.175.111:8080/workday/${month+"01"}/${month+"32"}").mkString
    val work_days = JSON.parseFull(json) match {
      case Some(map:List[String]) => map
    }
    work_days.sorted
  }

  def get_holiday(month:String):List[String]={
    val json = Source.fromURL(s"http://133.0.175.111:8080/holiday/${month+"01"}/${month+"32"}").mkString
    val work_days = JSON.parseFull(json) match {
      case Some(map:List[String]) => map
    }
    work_days.sorted
  }

//  val work_days = JSON.parseFull(json) match {
//    case Some(map:Map[String,List[String]]) => map
//  }
//
//  def get_work_days(month:String):List[String]={
//    val empty = List[String]()
//    work_days.getOrElse(month,empty).sorted
//  }
//  def get_holiday(m:String):List[String]={
//    val holiday = get_work_days(m)
//    val date = Calendar.getInstance()
//    val start_day = m+"01"
//    date.setTime(yyyyMMdd.parse(start_day))
//    date.roll(Calendar.DATE,-1)
//    val end_day = yyyyMMdd.format(date.getTime)
//    (start_day.toInt to end_day.toInt).map(String.valueOf).toList.diff(holiday).sortWith(_<_)
//  }

  def smooth(days:List[String]):List[List[String]]={
    var list = List[List[String]]()
    var j=0
    for(i<- 1 until days.length){
      if(days(i).toInt-days(i-1).toInt != 1){
        list = list:+days.take(i).drop(j)
        j=i
      }
    }
    list = list:+days.drop(j)
    list
  }


  def getCurrentDay():String = yyyyMMdd.format(new Date())
  def getCurrentMonth():String = yyyyMM.format(new Date())

  def get_yestoday(): String = {
    next_day(getCurrentDay(),-1)
  }

  def next_month(m:String,step:Int):String = {
    val date = Calendar.getInstance()
    date.setTime(yyyyMM.parse(m))
    date.add(Calendar.MONTH,step)
    yyyyMM.format(date.getTime)
  }
  def next_day(day:String,step:Int):String={
    val date = Calendar.getInstance()
    date.setTime(yyyyMMdd.parse(day))
    date.add(Calendar.DAY_OF_MONTH,step)
    yyyyMMdd.format(date.getTime)
  }
  def today(fmt:SimpleDateFormat=yyyyMMdd): String ={
    val date = Calendar.getInstance()
    fmt.format(date.getTime)
  }

  def get_prev_seven_day(day:String=null): Seq[String] = {
    val date = if (day==null) yyyyMMdd.format(new Date()) else day
    for(i <- 1 to 7) yield next_day(date,-1*i)
  }

  def get_prev_six_month(n:String=null): Seq[String] ={
    val m = if (n==null) yyyyMM.format(new Date()) else n
    for(i <- 1 to 6) yield next_month(m,i-7)
  }

  def get_prev_four_month(n:String=null): (String,String,String,String) ={
    val m = if (n==null) yyyyMM.format(new Date()) else n
    (next_month(m,-4),next_month(m,-3),next_month(m,-2),next_month(m,-1))
  }

  def get_four_month(n:String=null): (String,String,String,String) ={
    val m = if (n==null) yyyyMM.format(new Date()) else n
    (next_month(m,-3),next_month(m,-2),next_month(m,-1),m)
  }
  def get_three_month(n:String=null): (String,String,String) ={
    val m = if (n==null) yyyyMM.format(new Date()) else n
    (next_month(m,-2),next_month(m,-1),m)
  }

  def date_range(start:String,end:String):Seq[String] = {
    val s = yyyyMMdd.parse(start).getTime
    val e = yyyyMMdd.parse(end).getTime
    for(day <- s.to(e,24*3600*1000L)) yield yyyyMMdd.format(new Date(day))
  }

  def month_range(start:String,end:String): Seq[String] ={
    val start_i = start.toInt
    val end_i = end.toInt
    val year_range = start.take(4).toInt.to(end.take(4).toInt).toArray

    val range = year_range match {
      case Array(year) => start_i.to(end_i)
      case Array(year1,year2) => start_i.to((year1 + "12").toInt).toList:::(year2+"01").toInt.to(end_i).toList
    }
    range.map(_.toString)
  }

  def date_range_now(start:String) = date_range(start,today())

  def month_range_now(start:String)= month_range(start,today().take(6))

}