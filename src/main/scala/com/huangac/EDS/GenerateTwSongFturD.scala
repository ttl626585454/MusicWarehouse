package com.huangac.EDS

import com.huangac.util.SparkConfUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode

object GenerateTwSongFturD {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkConfUtil.getorCreateSparkSessionOnHive("local")
    import spark.implicits._
    import spark.sql
    val currentDay = 1
    val currentYear = 2022
    val currentMonth = 1


    sql("use musicwarehouse ")
    //当前日统计表
    sql(s"""
           |select
           |    songid as currentDay_songid,
           |    count(songid) as currentDay_song_cnt,
           |    0 as currentDay_supp_cnt,
           |    count(distinct uid) as  currentDay_usr_cnt,
           |    count(distinct order_id) as  currentDay_order_cnt
           |from musicwarehouse.to_client_song_play_operate_req
           |where year = ${currentYear} and month = ${currentMonth} and day = ${currentDay}
           |group by songid
           |""".stripMargin).createTempView("currentDayTable")
    //7天统计表
    sql(s"""
           |select
           |    songid as sevenDays_songid,
           |    count(songid) as sevenDays_song_cnt,
           |    0 as sevenDays_supp_cnt,
           |    count(distinct uid) as sevenDays_usr_cnt,
           |    count(distinct order_id) as sevenDays_order_cnt
           |from to_client_song_play_operate_req
           |where year = 2021 and month = 12 and day between 28 and 31 or year = 2022 and month = 1 and day = 1
           |group by songid
           |""".stripMargin).createTempView("sevenDaysTable")
//    //30天统计表
    sql(s"""
           |select
           |    songid as thirtyDays_songid,
           |    count(songid) as thirtyDays_song_cnt,
           |    0 as thirtyDays_supp_cnt,
           |    count(distinct uid) as thirtyDays_usr_cnt,
           |    count(distinct order_id) as thirtyDays_order_cnt
           |from to_client_song_play_operate_req
           |where year = 2021 and month = 12 and day between 2 and 31 or year = 2022 and month = 1 and day = 1
           |group by songid;""".stripMargin).createTempView("thirtyDaysTable")




    val currentDayTableDF = spark.read.table("currentDayTable")
    val sevenDaysTableDF = spark.read.table("sevenDaysTable")
    var thirtyDaysTableDF = spark.read.table("thirtyDaysTable")
    var songInfo = spark.read.table("musicwarehouse.tw_song_baseinfo_d")

    thirtyDaysTableDF.join(songInfo,thirtyDaysTableDF("thirtyDays_songid")===songInfo("nbr"),"inner").write.mode(SaveMode.Overwrite).saveAsTable("tw_thirtyDaysTableInfo_temp")
//      createTempView("thirtyDaysTableInfo")

//    currentDayTableDF.join(songInfo,currentDayTableDF("currentDay_songid")===songInfo("nbr"),"inner").show(10)
//    val t2s = thirtyDaysTableDF.join(sevenDaysTableDF, thirtyDaysTableDF("thirtyDays_songid") === sevenDaysTableDF("sevenDays_songid"), "left")
//    val t2s2c = t2s.join(currentDayTableDF, t2s("thirtyDays_songid") === currentDayTableDF("currentDay_songid"), "left")
//   .join(currentDayTableDF,thirtyDaysTableDF("thirtyDays_songid")===currentDayTableDF("currentDay_songid"),"left")
//      .join(songInfo,thirtyDaysTableDF("thirtyDays_songid")===songInfo("nbr"),"inner").show()
    //在本地运行时，无法进行最后一步连接，原因是内存不足
  }
}
