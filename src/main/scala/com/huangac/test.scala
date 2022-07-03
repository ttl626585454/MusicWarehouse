package com.huangac

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson2.JSONObject

object test {
  def main(args: Array[String]): Unit = {
//    val jSONArray = JSON.parseArray("[{\"name\":\"《中国好声音第二季 第十五期》\",\"id\":\"5ce5ffdb2a0b8b6b4707263e\"}]")
//    println(jSONArray.getJSONObject(0).getString("name"))
//    println(getSingerInfo("adsad"))

  }
/*  val getSingerInfo:String=>String =  (x:String)=>{
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

  }*/



}
