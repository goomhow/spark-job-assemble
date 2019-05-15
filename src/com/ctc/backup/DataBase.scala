package com.ctc.backup

trait DataBase extends Serializable{
  def importByCsv(path:String,table:String)
  def exportByCsv(path:String,table:String,partitionColumn:String,start:Int,end:Int)
  def simpleExportByCsv(path:String,table:String)
}
