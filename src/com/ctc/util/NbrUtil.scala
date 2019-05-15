package com.ctc.util

object NbrUtil {
  def normalize_phone(phone:String, area:String="", phone_suffix:String=""): String ={
    if(phone==null || phone.isEmpty){
      return ""
    }
    val length = phone.length
    if(length == 11 && phone.startsWith("1") && phone(1).toInt>2){
      return phone
    }else if(length==12 && phone.startsWith("01") && phone(2).toInt>2){
      return phone.drop(1)
    }else if(length==13 && phone.startsWith("861") && phone(3).toInt>2){
      return phone.drop(2)
    }

    // 虚拟网小号则取完整手机号码
    if (phone_suffix!=null && phone_suffix.length == 11 && phone_suffix.startsWith("1") && phone_suffix(1).toInt>2)
      return phone_suffix

    // 17901/17909
    var mob= if(phone.startsWith("17901") || phone.startsWith("17909"))
      phone.drop(5)
    else
      phone

    // 去除国家代码
    val NBR_GJ = "^0{2,}86(\\d+)".r
    val NBR_GJ(a:String) = mob
    mob = a
    // 固话加上区号
    if(mob.length>=7 && mob.length<=8 && !mob.startsWith("0")){
       area + mob
    }else{
      mob
    }
  }
}
