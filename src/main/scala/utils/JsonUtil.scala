package utils

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * json解析工具
  */
object JsonUtil {

    /**
      * 通过json串和key值获取value值
      * @param jsonString json串儿
      * @param key 获取的key
      * @return 得到 value
      */
    def getString(jsonString: String, key: String): String = {
        //通过json串获取json对象
        val json: JSONObject = JSON.parseObject(jsonString)

        //通过key值到其中去找value
        val ans: String = json.getString(key)

        //返回value值
        ans
    }

}
