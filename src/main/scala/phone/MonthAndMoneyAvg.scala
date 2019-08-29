package phone

import org.apache.spark.rdd.RDD
import utils.Data2RedisUtil

object MonthAndMoneyAvg {
    def getAns(args: Any*): Unit = {
        //id, phonenumber,money, date, lat, log
        val logs: RDD[(String, String, Double, String, String, String)] = args(0).asInstanceOf[RDD[(String, String, Double, String, String, String)]]
        val monthAndMoney: RDD[((String, String), (Double, Int))] = logs.map(x => {
            val dateTime: String = x._4.substring(5, 7)

            ((x._2, dateTime), (x._3, 1))
        })

        val ans: RDD[((String, String), (Double, Int))] = monthAndMoney.reduceByKey((x, y) => {
            (x._1 + y._1, x._2 + y._2)
        })

        Data2RedisUtil.doDataSave(ans, "monthAndMoneyAvg")

    }

}
