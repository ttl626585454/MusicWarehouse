package com.huangac.ODS

import com.huangac.outoutFormat.KeyNameAsFileName
import com.huangac.util.SparkConfUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

/*
* 每一个事件都生成一个文件,存入hdfs中，格式为text
*
*
*
* */
object Currentday_clientlog {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkConfUtil.getorCreateSparkSession("local")
    import spark.implicits._
    import spark.sql
    val sc = spark.sparkContext
    val lines:RDD[String] = sc.textFile("currentday_clientlog.tar.gz")
    //清洗无效的行
    val validLines = lines.filter(line => line.split("&").size == 6)

    /*
    * 1575302368&99702&MINIK_CLIENT_SONG_PLAY_OPERATE_REQ&{"songid": "LX_217898", "mid": 99702, "optrate_type": 2, "uid": 49915635, "consume_type": 0, "play_time": 0, "dur_time": 0, "session_id": 14089, "songname": "那女孩对我说", "pkg_id": 4, "order_id": "InsertCoin_43347"}&3.0.1.15&2.4.4.30
    *
    *
    * */
    validLines.map(str=>{
      var sts = str.split("&")
      (sts(2),sts(3))
    }).saveAsHadoopFile("hdfs://192.168.247.129:9000/musicWarehouse/ODS",
      classOf[String],
      classOf[String],
      classOf[KeyNameAsFileName])

  }
}
