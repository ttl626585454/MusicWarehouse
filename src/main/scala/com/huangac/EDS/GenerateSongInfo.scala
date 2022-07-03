package com.huangac.EDS

import com.alibaba.fastjson.{JSON, JSONArray}
import com.alibaba.fastjson2.JSONObject
import com.huangac.common.DateUtils
import com.huangac.util.SparkConfUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

/*从ods中清洗出 歌曲特征日统计表*/
/**
 *  生成TW层 TW_SONG_BASEINFO_D 数据表 parquet格式
 *    主要是读取Hive中的ODS层 TO_SONG_INFO_D 表生成 TW层 TW_SONG_BASEINFO_D表，
 *
 */
object GenerateSongInfo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkConfUtil.getorCreateSparkSessionOnHive("local")
    import spark.implicits._
    import spark.sql
    val songInfoDF = spark.read.table("musicwarehouse.to_song_info_d")
//      .withColumn()
    //注册udf
    val getAlbumNameUdf = udf(getAlbumName)
    val getPostTimeUdf = udf(getPostTime)
    val getSingerInfoUdf = udf(getSingerInfo)

    /*得到数据清洗后的DF*/
    songInfoDF.withColumn("album",getAlbumNameUdf(col("album")))
      .withColumn("post_time",getPostTimeUdf(col("post_time")))
      .withColumn("singer_info",getSingerInfoUdf(col("singer_info")))
      .withColumn("lyricist",getSingerInfoUdf(col("lyricist")))
      .withColumn("composer",getSingerInfoUdf(col("composer")))
      .withColumn("ori_singer",getSingerInfoUdf(col("ori_singer")))
      .write.saveAsTable("musicwarehouse.TW_SONG_BASEINFO_D")
  }

  /*获取专辑的名称，对专辑字段进行清洗 只要专辑的名字*/
  val getAlbumName = (str:String)=>{
    var albumName = ""
    try{
      val jSONArray = JSON.parseArray(str)
      albumName =  jSONArray.getJSONObject(0).getString("name")
    }catch {
      case e:Exception=>{
        if (str.contains("《")&&str.contains("》")){
          albumName = str.substring(str.indexOf('《'),str.indexOf('》')+1)

        }else{
          albumName = "无专辑"
        }
      }
    }
    albumName
  }
  /*对发行时间的清洗*/
  val  getPostTime:String=>String = (x:String)=>{
    DateUtils.formatDate(x)
  }

  //对歌手信息进行清洗,歌手直接，号相隔
  val getSingerInfo:String=>String =  (x:String)=>{
    var result:StringBuffer = new StringBuffer
    try {
      val jSONArray = JSON.parseArray(x) //得到JSON数组
      val arraySize = jSONArray.size()-1
      var count = arraySize
      while (count>=0){
        val str = jSONArray.getJSONObject(count).getString("name")
        result.append(str+',')
        count-=1
      }
      result.setLength(result.length()-1)
      result.toString
    }catch {
      //如果为空或者处理异常，返回原字符串
      case e:Exception=>{
        x.toString
      }
    }
  }
}
