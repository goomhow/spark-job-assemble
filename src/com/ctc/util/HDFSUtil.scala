package com.ctc.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.sys.process.stringToProcess
object HDFSUtil extends Serializable {
  private[this] var WORK_PATH = new Path("/user/spark")
  val BD_MODEL_DATA_PATH = s"model/bd_data/%s"
  val BD_DATA_PATH = "td_work/bns_kd_list/%s"
  val CDMA_MODEL_DATA_PATH = s"model/cdma_data/%s"
  val CDMA_DATA_PATH = "td_work/tc_cdma_qs/%s"
  val BROAD_BAND_FLUX_PATH = "info/broadband_flux/%s"
  val BROAD_BAND_AGG_PATH = "td_work/broadband_flux/%s"

  val PRD_PRD_INST_EXT_MON = "pv_data_z/PRD_PRD_INST_EXT_MON/%s"

  val BAS_PRD_INST = "pv_mart_z/bas_prd_inst/%s"
  val hdfs=FileSystem.get(new Configuration())
  def exists(name:String):Boolean={
    hdfs.exists(new Path(name))
  }

  def rename(srcPath:String,targetPath:String): Boolean ={
    if(!exists(targetPath)){
      hdfs.rename(new Path(srcPath),new Path(targetPath))
    }else{
      false
    }
  }

  def getChildrenDir(path:String):Array[String]={
    hdfs.listStatus(new Path(path)).map(_.getPath.toString)
  }

  def setWorkPath(path:String): Unit ={
    WORK_PATH=new Path(path)
    println(s"工作目录已经设置为$path")
    hdfs.setWorkingDirectory(WORK_PATH)
  }

  def removeFile(path:String):Int = {
    val abs_path = WORK_PATH.getName+"/"+path
    if(path.matches("^(/?\\w+/?)+$"))
      "hdfs dfs -rm %s".format(abs_path) !
    else
      throw new Exception(s"请检查路径${abs_path}是否正确")
  }

  def removeDir(path:String):Int = {
    val abs_path = WORK_PATH.getName+"/"+path
    if(path.matches("^(/?\\w+/?)+$"))
      "hdfs dfs -rm -r %s".format(abs_path) !
    else
      throw new Exception(s"请检查路径${abs_path}是否正确")
  }

  def delete(path:String,recursive:Boolean):Boolean={
    val p = new Path(path)
    hdfs.delete(p,recursive)
  }

  def isNewOnMonth(path:String):Boolean = {
    val current = DateUtil.getCurrentMonth().toInt
    val p = new Path(path)
    val stat = hdfs.getFileStatus(p)
    val fileDate = DateUtil.yyyyMM.format(stat.getModificationTime).toInt
    fileDate >= current
  }

  def uploadFile(fpath:String,hdfs_dir:String="tmp"):String={
    val src = new Path(fpath)
    val dst_path = "%s/%s".format(hdfs_dir,fpath.split("/").takeRight(1)(0))
    val dst = new Path(dst_path)
    hdfs.copyFromLocalFile(false,true,src,dst)
    dst_path
  }

  def downloadHDFSFile(hdfs_path:String,local_path:String): Unit ={
    val src = new Path(hdfs_path)
    val dst = new Path(local_path)
    hdfs.copyToLocalFile(false,src,dst,true)
  }

  def getTmpPath(fpath:String): String ={
    "%s/%s".format("tmp",fpath.split("/").takeRight(1)(0))
  }

  def getLocalPath(path:String): String ={
    path match {
      case i if i.startsWith("/") => "file:///"+path
      case i if i.startsWith("./") => "file:////home/spark" + i.drop(1)
      case i => "file:////home/spark/" + i
    }
  }
}
