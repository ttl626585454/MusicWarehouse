package com.huangac

import com.huangac.util.SparkConfUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import com.huangac.ODS.util.Line2Event_JSONofODS
import com.huangac.outoutFormat.KeyNameAsFileName
object step1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkConfUtil.getorCreateSparkSession("local")
    import spark.implicits._
    import spark.sql
    val sc = spark.sparkContext
    val lines:RDD[String] = sc.textFile("C:\\Users\\JimRaynor\\Desktop\\马士兵学习笔记\\二阶段\\音乐数仓\\datawarehouse-master\\音乐数据中心平台03-项目介绍\\currentday_clientlog.tar.gz")
    //清洗无效的行
    val validLines = lines.filter(line => line.split("&").size == 6)

    /*取得歌曲事件,按照数据事件名为文件名生成文件
    * 源 事件字符串为
    * 1575302368&99702&MINIK_CLIENT_SONG_PLAY_OPERATE_REQ&{"songid": "LX_217898", "mid": 99702, "optrate_type": 2, "uid": 49915635, "consume_type": 0, "play_time": 0, "dur_time": 0, "session_id": 14089, "songname": "那女孩对我说", "pkg_id": 4, "order_id": "InsertCoin_43347"}&3.0.1.15&2.4.4.30
    *
    *
    * */
    val fileterRDD:RDD[String] = validLines.filter(x => {
      x.split("&")(2).equals("MINIK_CLIENT_SONG_PLAY_OPERATE_REQ")
    })
    fileterRDD.map(line=>{
      Line2Event_JSONofODS.line2Event_JSON(line)
    }).saveAsHadoopFile("hdfs://192.168.247.129:9000/musicWarehouse/ODS",
      classOf[String],
      classOf[String],
      classOf[KeyNameAsFileName])


    //saveAsHadoopFile("",classOf[String],classOf[String],classOf[KeyNameAsFileName])
  }
}
