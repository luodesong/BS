package busoverview

import org.apache.spark.rdd.RDD
import utils.Data2RedisUtil

object OrderAndMin {
    def getAns (agrs: Any*): Unit = {
        //省份，日期，小时，分钟，提交价钱，是否成功，提交一次的标记
        val datasTuple: RDD[(String, String, String, String, Double, Int, Int)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, Double, Int, Int)]]
        val minuteAndOrders: RDD[(String, (Double, Int, Int))] = datasTuple.map(x => {
            (x._4, (x._5, x._6, x._7))
        })
        Data2RedisUtil.doDataSave(minuteAndOrders, "orderAndMin")
    }

}
