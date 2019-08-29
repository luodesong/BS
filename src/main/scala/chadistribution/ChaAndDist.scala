package chadistribution


import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import utils.{Data2MysqlUtil, JdbcConnectionPool, JedisConnectionPool, JsonUtil}

object ChaAndDist {

    def getAns(agrs: Any*): Unit = {
        //省份，日期，小时，分钟，提交价钱，是否成功，提交一次的标记
        val datasTuple: RDD[(String, String, String, String, Double, Int, Int)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, Double, Int, Int)]]
        val hourAndCharInfo: RDD[(String, String, String, String, Double, Int, Int)] = datasTuple.map(x => x).filter(_._6 == 1)
        val hourAndCharInfoRDD: RDD[(String, (Double, Int))] = hourAndCharInfo.map(x => {
            (x._3, (x._5, 1))
        })

        val ans: RDD[(String, (Double, Int))] = hourAndCharInfoRDD.reduceByKey((x, y) => {
            (x._1 + y._1, x._2 + y._2)
        })
        Data2MysqlUtil.doDataSave(ans,"chaAndDist")
    }
}
