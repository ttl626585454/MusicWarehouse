package com.huangac.outoutFormat

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class KeyNameAsFileName extends MultipleTextOutputFormat[Any,Any]{
  /*key 代表传入的key，name 代表默认文件名字*/
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    var fileName = key.asInstanceOf[String]
    fileName
  }

  /*指定文件内容输出key，这里设置不输出key*/
  override def generateActualKey(key: Any, value: Any): Any = {
    null
  }

}
