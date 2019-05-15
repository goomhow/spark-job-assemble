package com.ctc.util

import java.util.Properties
import org.apache.spark.sql.types._
import sys.env

object INSTANTCE extends Serializable {

  val SEP = ","

  val CTC_NBR = "^(133|153|170|173|177|180|181|189|199)\\d{8}$"
  val MOB_NBR = "^1[2-9]\\d{9}$"

  val anti_url = env.getOrElse("ANTI_JDBC_URL","")
  val anti_url2 = env.getOrElse("ANTI_JDBC_URL2","")
  val msg_url = env.getOrElse("MSG_JDBC_URL2","")

  val properties = new Properties()
  properties.setProperty("defaultRowPrefetch", "10000")

  val TD_URL = env.getOrElse("TD_JDBC_URL2","")
  val TD_PROPERTY = new Properties()
  TD_PROPERTY.setProperty("driver", "com.teradata.jdbc.TeraDriver")
  TD_PROPERTY.setProperty("user", ENV.getOrElse("TD_USER",""))
  TD_PROPERTY.setProperty("password", ENV.getOrElse("TD_PASSWORD",""))
  TD_PROPERTY.setProperty("fetchsize", "50000")

  val AREA_LATN = Array(
    ("1001", "武汉", "027"),
    ("1003", "襄樊", "0710"),
    ("1004", "黄冈", "0713"),
    ("1005", "宜昌", "0717"),
    ("1006", "孝感", "0712"),
    ("1007", "鄂州", "0711"),
    ("1008", "咸宁", "0715"),
    ("1009", "十堰", "0719"),
    ("1010", "荆门", "0724"),
    ("1011", "黄石", "0714"),
    ("1012", "随州", "0722"),
    ("1013", "恩施", "0718"),
    ("1014", "仙桃", "0728"),
    ("1015", "天门", "0728"),
    ("1016", "潜江", "0728"),
    ("1017", "神农架", "0719"),
    ("1018", "荆州", "0716")
  )
  val corp_map = Map(
    "ZJ" -> "省质监局",
    "YZ" -> "邮储银行",
    "DS" -> "省地税局",
    "AJ" -> "省安监",
    "DX" -> "省电信公司"
  )
  val user_com_struct = StructType(
    StructField("BILLING_NBR", StringType, false) ::
      StructField("COMPANY", StringType, false) :: Nil)
  val uc_struct = StructType(
    StructField("BILLING_NBR", StringType, false) ::
      StructField("CELL_ID", StringType, false) :: Nil)
  val ucc_struct = StructType(
    StructField("BILLING_NBR", StringType, false) ::
      StructField("CELL_ID", StringType, false) ::
      StructField("CELL_COUNT", IntegerType, false) :: Nil)
  val juan_struct = StructType(
    StructField("NBR", StringType, false) ::
      StructField("OPP_NBR", StringType, false) ::
      StructField("CNT", IntegerType, false) :: Nil)
  val com_struct = StructType(
    StructField("COMPANY", StringType, false) ::
      StructField("CELL_ID", StringType, false) :: Nil)
  val voice_struct = StructType(
    StructField("BILLING_NBR", StringType, false) ::
      StructField("SELF_CELL_ID", StringType, false) ::
      StructField("START_TIME", StringType, false) :: Nil)
  val lte_struct = StructType(
    StructField("BILLING_NBR", StringType, false) ::
      StructField("CELLID", StringType, false) ::
      StructField("START_TIME", StringType, false) :: Nil)
  val USER_POSITION_STRUCT = StructType(
    StructField("START_TIME", StringType, false) ::
      StructField("BILLING_NBR", StringType, false) ::
      StructField("CELL_ID", StringType, false) :: Nil)

  val corp_cell_struct = StructType(
    StructField("CORP", StringType) ::
      StructField("COMPANY", StringType) ::
      StructField("CELL_ID", StringType) ::
      StructField("LNG", DoubleType) ::
      StructField("LAT", DoubleType) :: Nil
  )
  val company_cell_struct = StructType(
    StructField("COMPANY", StringType) ::
      StructField("CELL_ID", ArrayType(StringType)) ::
      StructField("LNG", DoubleType) ::
      StructField("LAT", DoubleType) :: Nil
  )
  val filter = StructType(
    StructField("BILLING_NBR", StringType, false) :: Nil)
  val month_call_pair = StructType(
    StructField("BILLING_NBR", StringType) ::
      StructField("OPP_NBR_CNT", MapType(StringType, IntegerType)) :: Nil
  )
  val label_struct = StructType(StructField("BILLING_NBR", StringType) ::
    StructField("IS_OTHER_NET", IntegerType) ::
    StructField("LABEL", IntegerType) ::
    StructField("CORP", StringType) ::
    StructField("CORP_NBR_CNT", IntegerType) :: Nil
  )
  val label_struct_new = StructType(
    StructField("BILLING_NBR", StringType) ::
      StructField("IS_OTHER_NET", IntegerType) ::
      StructField("CORP", StringType) :: Nil
  )

  val UM_STRUCT = StructType(
    StructField("ACC_NBR", StringType, true) ::
      StructField("IS_OTHER_NET", StringType, true) ::
      StructField("CUST_NAME", StringType, true) ::
      StructField("NBR_AREA_CODE", StringType, true) ::
      StructField("NBR_AREA_NAME", StringType, true) :: Nil)

  val tm = StructType(
    StructField("SHEET_NBR", StringType, true) ::
      StructField("REGION_NAME", StringType, true) ::
      StructField("CUST_ID", DecimalType(12, 0), true) ::
      StructField("CUST_NAME", StringType, true) ::
      Nil)

  val all_cell_struct = StructType(
    StructField("AREA_NAME", StringType) ::
      StructField("CELL_ID", StringType) ::
      StructField("LNG", DoubleType) ::
      StructField("LAT", DoubleType) :: Nil
  )

  val tmp_nbr = StructType(
    StructField("ID", StringType, false) ::
      StructField("BILLING_NBR", StringType, false) :: Nil)
  val tmp_juan_nbr = StructType(
    StructField("ID", StringType, false) ::
      StructField("NBR", StringType, false) ::
      StructField("OPP_NBR", StringType, false) :: Nil)

  case class Hcode(prefix:String,city_code:String,city_name:String,net_type:String)


  val ALL_LOCALNETS = Array("snj", "yc", "tm", "xg", "hg", "sy", "es", "xf", "sz", "xn", "jm", "jz", "xt", "qj"
    , "hs", "wh", "ez")
  val RE_COUNTRY_CODE = raw"^0{2,}86".r

  // 规格化电话号码
  def normalizePhoneNumber(rawPhone: String, area: String = "", phoneSuffix: String = ""): String = {
    var phone = rawPhone
    val length = phone.length

    if (phone.isEmpty)
      return ""


    if (length == 11) {
      if (phone(0) == '1' && "345789".contains(phone(1)))
        return phone
    } else if (length == 12) {
      if (phone.substring(0, 2) == "01" && "345789".contains(phone(2)))
        return phone.substring(1)
    } else if (length == 13) {
      if (phone.substring(0, 3) == "861" && "345789".contains(phone(3)))
        return phone.substring(2)
    } else if (length < 7) {
      return phone
    }

    // 虚拟网小号则取完整手机号码
    if (phoneSuffix.length == 11 && phoneSuffix(0) == '1' && "345789".contains(phoneSuffix(1)))
      return phoneSuffix

    // 17901/17909
    if (phone.substring(0, 4) == "1790" && "19".contains(phone(4)))
      phone = phone.substring(5)

    // 去除国家代码
    phone = RE_COUNTRY_CODE.replaceFirstIn(phone, "")

    // 固话加上区号
    if ((length == 7 || length == 8) && phone(0) != '0')
      return area + phone

    phone
  }

}

//CORPORRATE_NAME!='省电信公司'