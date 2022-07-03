package com.huangac.ODS.util

import com.alibaba.fastjson.{JSON, JSONObject}

/*
* 1575302368&99702&MINIK_CLIENT_SONG_PLAY_OPERATE_REQ&{"songid": "LX_217898", "mid": 99702, "optrate_type": 2, "uid": 49915635, "consume_type": 0, "play_time": 0, "dur_time": 0, "session_id": 14089, "songname": "那女孩对我说", "pkg_id": 4, "order_id": "InsertCoin_43347"}&3.0.1.15&2.4.4.30
* 输出
* (MINIK_CLIENT_SONG_PLAY_OPERATE_REQ,LX_217898	 99702	 2	 49915635	 0	 0	 0	 14089	那女孩对我说	 4	InsertCoin_43347)

* 不适合所有line
* */
object Line2Event_JSONofODS {
  def line2Event_JSON(line:String):(String,String)={
    var strs:Array[String] = line.split("&")  //输入字符串分隔，为下方提取元素做准备
    var jsonMap = strs(3).substring(1,strs(3).length-1).split(",").map(
      x=>{
        var temp = x.split(":")
        if (temp.length<2)
          (temp(0),"空")
        else
        (temp(0),temp(1))
      }
    )
    var cmd:String = strs(2)            //输出key
    var stringBuffer =new StringBuffer()//拼接输出value
    jsonMap.foreach(x=>{
      var out = x._2
      if (x._2(1) == '\"') {
        out = x._2.substring(2,x._2.length-1)
      } //去除双引号
      stringBuffer.append(out+"\t")
    })
    stringBuffer.setLength(stringBuffer.length()-1)  //去除末尾的 \t
    (cmd,stringBuffer.toString)
  }
//
//  def main(args: Array[String]): Unit = {
//   var str =  line2Event_JSON("1575302368&99702&MINIK_CLIENT_SONG_PLAY_OPERATE_REQ&{\"songid\": \"LX_217898\", \"mid\": 99702, \"optrate_type\": 2, \"uid\": 49915635, \"consume_type\": 0, \"play_time\": 0, \"dur_time\": 0, \"session_id\": 14089, \"songname\": \"那女孩对我说\", \"pkg_id\": 4, \"order_id\": \"InsertCoin_43347\"}&3.0.1.15&2.4.4.30")
//    println(str)
//  }

}
