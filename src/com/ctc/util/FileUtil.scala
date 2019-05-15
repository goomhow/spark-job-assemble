package com.ctc.util

import java.io.{FileWriter, RandomAccessFile}

object FileUtil {
  def saveCsv(path:String,list:List[String]): Unit ={
    val output = new FileWriter(path)
    output.write(list.mkString("\n"))
  }

}
