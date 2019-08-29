package phone

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import utils.{Data2MysqlUtil, Data2RedisUtil}

object DayAndMoney {
    def getAns(args: Any*): Unit = {
        //id, phonenumber,money, date, lat, log
        val logs: RDD[(String, String, Double, String, String, String)] = args(0).asInstanceOf[RDD[(String, String, Double, String, String, String)]]
        val dayAndMoney: RDD[(String, Double)] = logs.map(x => {
            val dateTime: String = x._4.substring(8, 10)
            (dateTime, x._3)
        })
        val ans: RDD[(String, Double)] = dayAndMoney.reduceByKey(_+_)
        Data2RedisUtil.doDataSave(ans, "dayAndMoney")

    }
}
