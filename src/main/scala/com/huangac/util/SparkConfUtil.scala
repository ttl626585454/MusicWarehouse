package com.huangac.util

import org.apache.spark.sql.SparkSession

object SparkConfUtil {
  var spark:SparkSession = null;
  var spark_on_hive:SparkSession = null;
  def getorCreateSparkSession(master:String="local"):SparkSession = {
    if (spark!=null)
      return spark
    if (spark==null){
      synchronized {
        if (spark == null)
            spark = SparkSession.builder().master(master).appName("build").getOrCreate()
        return spark
      }
    }
    spark
  }

  def getorCreateSparkSessionOnHive(master:String,wareHouseDir:String="hdfs://192.168.247.129:9000/user/hive/warehouse",thrifturl:String="thrift://192.168.247.129:9083"): SparkSession ={
    if (spark_on_hive!=null)
      return spark_on_hive
    if (spark_on_hive == null){
      synchronized{
        if (spark_on_hive == null)
          spark_on_hive = SparkSession.builder()
            .master(master)
            .appName("sparkonhive")
            .config("spark.sql.warehouse.dir",wareHouseDir)
            .config("hive.matastore.uris",thrifturl)
            .enableHiveSupport()
            .getOrCreate()
            spark_on_hive
      }
    }
    spark_on_hive
  }



}
