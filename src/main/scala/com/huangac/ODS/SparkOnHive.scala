package com.huangac.ODS

import com.huangac.util.SparkConfUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

/*如果使用 spqk.sql.write.saveAstable() 则在hive中存储的是parquet格式的数据*/
object SparkOnHive {
  val spark:SparkSession = SparkConfUtil.getorCreateSparkSessionOnHive("local")


  def main(args: Array[String]): Unit = {
    /*设置默认分区*/
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    import spark.implicits._
    import spark.sql
    Logger.getLogger("org").setLevel(Level.WARN)
    var logDF:DataFrame = spark.read.json("hdfs://192.168.247.129:9000/musicWarehouse/ODS/MINIK_CLIENT_SONG_PLAY_OPERATE_REQ")
      .withColumn("year",lit(2022))
      .withColumn("month",lit(1))
      .withColumn("day",lit(1))
    sql("use musicwarehouse")
//    logDF.write.saveAsTable("TO_CLIENT_SONG_PLAY_OPERATE_REQ")
    //在表中添加分区字段
    logDF.write.partitionBy("year","month","day").format("hive").saveAsTable("musicwarehouse.TO_CLIENT_SONG_PLAY_OPERATE_REQ")
    sql("select * from musicwarehouse.TO_CLIENT_SONG_PLAY_OPERATE_REQ where year=2022").show()




  }
  






}
