package com.ctc.redis
import scala.sys.env
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis
import com.ctc.redis.RedisGaodeData._
import com.ctc.util.INSTANTCE.{anti_url,properties}

class RedisGaodeData(jedis: Jedis) {
/*  def saveToOracle(spark:SparkSession): Unit ={
    val data = jedis.lrange(DATA_NAME,0,1)
    val ds = spark.createDataFrame(data)
    val df = spark.read.json(ds)
    df.toDF(df.columns.map(_.toUpperCase):_*).write.mode("append").jdbc(anti_url,"TM_GAO_DE_DATA",properties)
  }*/
}
object RedisGaodeData{
  val JOB_NAME = "GAO_DE_JOB"
  val DATA_NAME = "GAO_DE_DATA"
  val HOST = "133.0.189.11"
  val PORT = 6379

  private val PASSWORD = env.get("DEFAULT_REDIS_PASSWORD") match {
    case Some(a:String) => a
  }

  def apply(): RedisGaodeData = {
    val jedis = new Jedis(HOST,PORT)
    jedis.auth(PASSWORD)
    new RedisGaodeData(jedis)
  }
}
