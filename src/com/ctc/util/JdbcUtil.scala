package com.ctc.util

import java.io.{ FileWriter, PrintWriter}
import java.sql._

import com.ctc.util.INSTANTCE._

object JdbcUtil {

  def callProcedure(procedureName:String): Boolean = {
    Class.forName("oracle.jdbc.driver.OracleDriver")
    var conn: Connection = null
    var cs: CallableStatement = null
    try {
      conn = DriverManager.getConnection(anti_url, properties)
      cs = conn.prepareCall(s"call $procedureName()")
      cs.execute()
      true
    } catch {
      case e: Exception => e.printStackTrace(); false
    } finally {
      if (cs != null) cs.close()
      if (conn != null) conn.close()
    }
  }

  def callProcedure(procedureName:String,inParams:Object*): Boolean = {
    val sql = s"call $procedureName(${"?"*inParams.length mkString(",")})"
    Class.forName("oracle.jdbc.driver.OracleDriver")
    var conn: Connection = null
    var cs: CallableStatement = null
    try {
      println("* START RUN PROCEDURE:"*10 + sql + "*"*10)
      conn = DriverManager.getConnection(anti_url, properties)
      cs = conn.prepareCall(sql)
      for((v,index) <- inParams.zipWithIndex){
        cs.setObject(index+1,v)
      }
      cs.execute()
      println("* SUCCESS RUN PROCEDURE:"*10 + sql + "*"*10)
      true
    } catch {
      case e: Exception => e.printStackTrace()
        println("* FAILED RUN PROCEDURE:"*10 + sql + "*"*10)
        false
    } finally {
      if (cs != null) cs.close()
      if (conn != null) conn.close()
    }
  }

  def getMaxMin(td_table:String):(Int,Int)={
    val sql = s"SELECT MAX(Date_id),MIN(Date_id) from $td_table"
    Class.forName("com.teradata.jdbc.TeraDriver")
    var conn:Connection=null
    var statmenet:Statement = null
    try{
      conn = DriverManager.getConnection(TD_URL,TD_PROPERTY)
      statmenet = conn.createStatement()
      val rs = statmenet.executeQuery(sql)
      rs.next()
      (rs.getString(1).toInt,rs.getString(2).toInt)
    }catch{
      case e:Exception => e.printStackTrace(); null
    }finally {
      if(statmenet!=null) statmenet.close()
      if(conn!=null) conn.close()
    }
  }

  def getTdCsvAsList(sql:String):List[String]={
    Class.forName("com.teradata.jdbc.TeraDriver")
    var conn:Connection=null
    var statmenet:Statement = null
    try{
      conn = DriverManager.getConnection(TD_URL,TD_PROPERTY)
      statmenet = conn.createStatement()
      val rs = statmenet.executeQuery(sql)
      val meta = rs.getMetaData
      val column_size = meta.getColumnCount()
      val column_names = for(i <- 1 to column_size)yield meta.getColumnName(i)
      var list = List[String](column_names.mkString(","))
      while (rs.next){
        val x = for( i<- 1 to column_size)yield rs.getObject(i)
        list = list:+x.mkString(",")
      }
      list
    }catch{
      case e:Exception => e.printStackTrace();null
    }finally {
      if(statmenet!=null) statmenet.close()
      if(conn!=null) conn.close()
    }
  }

  def saveTdAsCsv(path:String,sql:String,sep:String="|"): Unit ={
    Class.forName("com.teradata.jdbc.TeraDriver")
    var conn:Connection=null
    var statmenet:Statement = null
    var file:PrintWriter = null
    try{
      conn = DriverManager.getConnection(TD_URL,TD_PROPERTY)
      statmenet = conn.createStatement()
      file =new PrintWriter(new FileWriter(path))
      val rs = statmenet.executeQuery(sql)
      val meta = rs.getMetaData
      val column_size = meta.getColumnCount()
      val column_names = for(i <- 1 to column_size)yield meta.getColumnName(i)
      file.write(column_names.mkString(sep)+"\n")
      var c = 0
      while (rs.next){
        val x = for( i<- 1 to column_size)yield rs.getObject(i)
        file.write(x.mkString(sep)+"\n")
        if(c%10000==0)
          file.flush()
        c=c+1
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      if(statmenet!=null) statmenet.close()
      if(conn!=null) conn.close()
      if(file!=null) file.close()
    }
  }
}
