package charequest


import org.apache.spark.rdd.RDD
import utils.{Data2MysqlUtil}

object ProAndChaMoneyAndCount {

    def getProAndChaMoneyAndMinAndCount(agrs: Any*): Unit = {
        //省份，日期，小时，分钟，提交价钱，是否成功，提交一次的标记
        val datasTuple: RDD[(String, String, String, String, Double, Int, Int)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, Double, Int, Int)]]
        val provAndChaAndMinInfo: RDD[(String, String, String, String, Double, Int, Int)] = datasTuple.map(x => x).filter(_._6 == 1)
        val provAndChaAndMinRDD: RDD[((String, String), (Double, Int))] = provAndChaAndMinInfo.map(x => {
            ((x._1, x._4), (x._5,1))
        })
        val ans: RDD[((String, String),(Double, Int))] = provAndChaAndMinRDD.reduceByKey((x, y) => {
            (x._1 + y._1, x._2 + y._2)
        })
        Data2MysqlUtil.doDataSave(ans,"proAndChaMoneyAndMinAndCount")
    }
    def getProAndChaMoneyAndHourAndCount(agrs: Any*): Unit = {
        //省份，日期，小时，分钟，提交价钱，是否成功，提交一次的标记
        val datasTuple: RDD[(String, String, String, String, Double, Int, Int)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, Double, Int, Int)]]
        val provAndChaAndMinInfo: RDD[(String, String, String, String, Double, Int, Int)] = datasTuple.map(x => x).filter(_._6 == 1)
        val provAndChaAndMinRDD: RDD[((String, String), (Double, Int))] = provAndChaAndMinInfo.map(x => {
            ((x._1, x._3), (x._5,1))
        })
        val ans: RDD[((String, String),(Double, Int))] = provAndChaAndMinRDD.reduceByKey((x, y) => {
            (x._1 + y._1, x._2 + y._2)
        })
        Data2MysqlUtil.doDataSave(ans,"proAndChaMoneyAndHourAndCount")
    }
}
