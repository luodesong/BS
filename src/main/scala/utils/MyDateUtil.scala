package utils

import java.text.SimpleDateFormat
import java.util.Date

/**
  * 时间转化的工具类
  */
object MyDateUtil {

    /**
      * 用于计算两个时间的差值
      * @param string1 固定格式的时间字符串1
      * @param string2 固定格式的时间字符串1
      * @return 返回的是两个时间相差的毫秒数
      */
    def getSub(string1: String, string2: String): Long = {
        val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
        val date1: Date = simpleDateFormat.parse(string1)
        val date2: Date = simpleDateFormat.parse(string2)
        val ts1: Long = date1.getTime
        val ts2: Long = date2.getTime
        val l: Long = ts1 - ts2
        l
    }

    /**
      * 将固定格式的时间转化为毫秒数
      * @param string 固定格式的时间字符串
      * @return 毫秒数
      */
    def getTime(string: String): Long ={
        val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
        val date: Date = simpleDateFormat.parse(string)
        val ts: Long = date.getTime
        ts
    }

    /**
      * 将毫秒数转化为时间
      * @param long 毫秒数
      * @return 固定格式的时间
      */
    def getTimeString(long: Long): String ={
        val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
        val str: String = format.format(long)
        str
    }
}
