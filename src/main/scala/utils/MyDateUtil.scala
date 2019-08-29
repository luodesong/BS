package utils

import java.text.SimpleDateFormat
import java.util.Date

object MyDateUtil {

    def getSub(string1: String, string2: String): Long = {
        val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
        val date1: Date = simpleDateFormat.parse(string1)
        val date2: Date = simpleDateFormat.parse(string2)
        val ts1: Long = date1.getTime
        val ts2: Long = date2.getTime
        val l: Long = ts1 - ts2
        l
    }

    def getTime(string: String): Long ={
        val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
        val date: Date = simpleDateFormat.parse(string)
        val ts: Long = date.getTime
        ts
    }

    def getTimeString(long: Long): String ={
        val format: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
        val str: String = format.format(long)
        str
    }
}
