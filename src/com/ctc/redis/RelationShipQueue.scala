package com.ctc.redis

import redis.clients.jedis.Jedis

import sys.env
import com.ctc.redis.RelationShipQueue.JOB_NAME


class RelationShipQueue(val jedis:Jedis){

  def has_job: Boolean = jedis.llen(JOB_NAME) > 0

  def add_jobs(values:Seq[String]): Unit = {
    if(!jedis.isConnected){
      jedis.connect()
    }
    for(job <- values)
      jedis.lpush(JOB_NAME,job)
  }

  def add_job(job:String): Unit ={
    jedis.lpush(JOB_NAME,job)
  }

  def get_job(): String = {
    jedis.rpop(JOB_NAME)
  }

}

object RelationShipQueue{
  val JOB_NAME = "ZQ_JOB_LIST"
   val HOST = "133.0.189.11"
   val PORT = 6379
  private val PASSWORD = env.get("DEFAULT_REDIS_PASSWORD") match {
    case Some(a:String) => a
  }

  def apply(): RelationShipQueue = {
    val jedis = new Jedis(HOST,PORT)
    jedis.auth(PASSWORD)
    new RelationShipQueue(jedis)
  }

}
