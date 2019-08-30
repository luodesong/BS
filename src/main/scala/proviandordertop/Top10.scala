package proviandordertop

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.rdd.RDD
import utils.{Data2MysqlUtil}

/**
  * 以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中，进行前台页面显示。
  * 未完成
  */
object Top10 {
    def getAns(agrs: Any*): Unit = {
        val sc: SparkContext = agrs(1).asInstanceOf[SparkContext]
        //省份，日期，小时，分钟，提交价钱，是否成功，提交一次的标记
        val datasTuple: RDD[(String, String, String, String, Double, Int, Int)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, Double, Int, Int)]]
        val proAndHourAndSuccessAndAll: RDD[((String, String), (Int, Int))] = datasTuple.map(x => {
            ((x._1, x._3), (x._6, x._7))
        })
        val tump1: RDD[((String, String), (Int, Int))] = proAndHourAndSuccessAndAll.reduceByKey((x, y) => {
            (x._1 + y._1, x._2 + y._2)
        })
        val ans: Array[((String, String), String, Int)] = tump1.map(x => {
            val p: String = x._2._1 / x._2._2 * 100 + ";"
            (x._1, p, x._2._2)
        }).sortBy(_._3, false).take(10)

        val ansRDD: RDD[((String, String), String, Int)] = sc.makeRDD(ans)
        Data2MysqlUtil.doDataSave(ansRDD,"Top10")
    }

}
