package com.ctc.util

object MapUtil extends Serializable {
  val x_pi = 3.14159265358979324 * 3000.0 / 180.0
  // π
  val pi = 3.1415926535897932384626
  // 长半轴
  val a = 6378245.0
  // 偏心率平方
  val ee = 0.00669342162296594323
  //2KM近似经纬度
  val km_1lng=1000.0*(180.0/pi)/(a*math.sqrt(1-ee))*1000.0/863.0
  val km_1lat=1000.0*(180.0/pi)/a
  /**
     判断是否在国内，不在国内不做偏移
    :param lng:
    :param lat:
    :return:
    * */
  def out_of_china(lng:Double, lat:Double):Boolean={
    return ! (lng > 73.66 && lng < 135.05 && lat > 3.86 && lat < 53.55)
  }

  def _transformlat(lng:Double, lat:Double):Double={
    var ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat +
    0.1 * lng * lat + 0.2 * math.sqrt(math.abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
      math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * pi) + 40.0 *
      math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 *
      math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret
  }


  def _transformlng(lng:Double, lat:Double):Double= {
    var ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng +
    0.1 * lng * lat + 0.1 * math.sqrt(math.abs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
      math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * pi) + 40.0 *
      math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 *
      math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    ret
  }
/**
  火星坐标系(GCJ-02)转百度坐标系(BD-09)
  谷歌、高德——>百度
  :param lng:火星坐标经度
  :param lat:火星坐标纬度
  :return:
  * */
  def gcj02_to_bd09(lng:Double, lat:Double):Tuple2[Double,Double]={
    val z = math.sqrt(lng * lng + lat * lat) + 0.00002 * math.sin(lat * x_pi)
    val theta = math.atan2(lat, lng) + 0.000003 * math.cos(lng * x_pi)
    val bd_lng = z * math.cos(theta) + 0.0065
    val bd_lat = z * math.sin(theta) + 0.006
    (bd_lng, bd_lat)
  }

  /**
    百度坐标系(BD-09)转火星坐标系(GCJ-02)
    百度——>谷歌、高德
    :param bd_lat:百度坐标纬度
    :param bd_lon:百度坐标经度
    :return:转换后的坐标列表形式
    * */
  def bd09_to_gcj02(bd_lon:Double, bd_lat:Double):Tuple2[Double,Double]={
    val x = bd_lon - 0.0065
    val y = bd_lat - 0.006
    val z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * x_pi)
    val theta = math.atan2(y, x) - 0.000003 * math.cos(x * x_pi)
    val gg_lng = z * math.cos(theta)
    val gg_lat = z * math.sin(theta)
    (gg_lng, gg_lat)
  }

  /**
     WGS84转GCJ02(火星坐标系)
    :param lng:WGS84坐标系的经度
    :param lat:WGS84坐标系的纬度
    :return:
    * */
  def wgs84_to_gcj02(lng:Double, lat:Double):Tuple2[Double,Double]={
    // 判断是否在国内
    if (out_of_china(lng, lat)){
      (lng, lat)
    } else{
      var dlat = _transformlat(lng - 105.0, lat - 35.0)
      var dlng = _transformlng(lng - 105.0, lat - 35.0)
      val radlat = lat / 180.0 * pi
      var magic = math.sin(radlat)
      magic = 1 - ee * magic * magic
      val sqrtmagic = math.sqrt(magic)
      dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
      dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
      val mglat = lat + dlat
      val mglng = lng + dlng
      (mglng,mglat)
    }
  }


  /**
     GCJ02(火星坐标系)转GPS84
    :param lng:火星坐标系的经度
    :param lat:火星坐标系纬度
    :return:
    * */
  def gcj02_to_wgs84(lng:Double, lat:Double):Tuple2[Double,Double]={
    if (out_of_china(lng, lat)){
      (lng, lat)
    }else{
      var dlat = _transformlat(lng - 105.0, lat - 35.0)
      var dlng = _transformlng(lng - 105.0, lat - 35.0)
      var radlat = lat / 180.0 * pi
      var magic = math.sin(radlat)
      magic = 1 - ee * magic * magic
      var sqrtmagic = math.sqrt(magic)
      dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
      dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
      var mglat = lat + dlat
      var mglng = lng + dlng
      (lng * 2 - mglng, lat * 2 - mglat)
    }
  }


  def bd09_to_wgs84(lng:Double, lat:Double):Tuple2[Double,Double]={
    val (lng1, lat1)= bd09_to_gcj02(lng, lat)
    gcj02_to_wgs84(lng1, lat1)
  }



  def wgs84_to_bd09(lng:Double, lat:Double):Tuple2[Double,Double]= {
    val (lng1, lat1) = wgs84_to_gcj02(lng, lat)
    gcj02_to_bd09(lng1, lat1)
  }



/**
  * 计算百度坐标2点间距离
  * */
  def bd_distance(p1:(Double,Double),p2:(Double,Double)):Double={
    val (lng1,lat1) = p1
    val (lng2,lat2) = p2
    def fD(a:Double, b:Double, c:Double):Double={
      var a1 = a
      var b1 = b
      var c1 = c
      while(a1 > c1){
        a1 -= c1 - b1
      }
      while(a1 < b1){
        a1 += c1 - b1
      }
      a1
    }


    def jD(a:Double, b:Double, c:Double):Double={
      var a1 =  if(b!=0) math.max(a, b) else a
      a1 =  if(b!=0)math.min(a1, c) else a1
      a1
    }

    def yk(a:Double):Double={
      math.Pi * a / 180
    }

    def Ce(a:Double, b:Double, c:Double, d:Double):Double={
      val dO = 6370996.81
      dO * math.acos(math.sin(c) * math.sin(d) + math.cos(c) * math.cos(d) * math.cos(b - a))
    }

    def getDistance(lng1:Double,lat1:Double,lng2:Double,lat2:Double):Double={
      val lng_1 = fD(lng1, -180, 180)
      val lat_1 = jD(lat1, -74, 74)
      val lng_2 = fD(lng2, -180, 180)
      val lat_2 = jD(lat2, -74, 74)
      Ce(yk(lng_1), yk(lng_2), yk(lat_1), yk(lat_2))
    }

    getDistance(lng1, lat1, lng2, lat2)
  }

  def wgs84_distance(pos1:(Double,Double), pos2:(Double,Double)):Double={
    val p1 = wgs84_to_bd09(pos1._1, pos1._2)
    val p2 = wgs84_to_bd09(pos2._1, pos2._2)
    bd_distance(p1, p2)
  }

}
