package exam

import org.apache.spark.rdd.RDD
import utils.Data2MysqlUtil

object ExamStream {

    def getAns(agrs: Any*): Unit = {

        val logInfo: RDD[(String, String, String, String, String)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, String)]]
        //组装需要的数据
        val moneyRDD: RDD[(String, Double)] = logInfo.map(x => {
            (x._1.substring(0,8), x._5.toDouble)
        })

        //这一步是预先聚合的效果，能够完成一个批次提前聚合的效果，减少输出数据到数据库时候发生的网络io的次数
        val ans: RDD[(String, Double)] = moneyRDD.reduceByKey(_+_)
        //将数据传入到数据存储的类中去
        /**
          * 第一个参数是初步处理的数据
          * 第二个参数是要处理的业务逻辑的标志
          */
        ExemData2RedisUtil.doDataSave(ans,"money")
    }
    def getAns01(agrs: Any*): Unit = {

        val logInfo: RDD[(String, String, String, String, String)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, String)]]
        //组装需要的数据
        val frRDD: RDD[(String, Double)] = logInfo.map(x => {
            (x._3, 1)
        })

        //这一步是预先聚合的效果，能够完成一个批次提前聚合的效果，减少输出数据到数据库时候发生的网络io的次数
        val ans: RDD[(String, Double)] = frRDD.reduceByKey(_+_)
        //将数据传入到数据存储的类中去
        /**
          * 第一个参数是初步处理的数据
          * 第二个参数是要处理的业务逻辑的标志
          */
        ExemData2RedisUtil.doDataSave(ans,"count")
    }

    def getAns02(agrs: Any*): Unit = {

        val logInfo: RDD[(String, String, String, String, String)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, String)]]
        //组装需要的数据
        val locRDD: RDD[(String, Double)] = logInfo.map(x => {
            (x._2, x._5.toDouble)
        })

        //这一步是预先聚合的效果，能够完成一个批次提前聚合的效果，减少输出数据到数据库时候发生的网络io的次数
        val ans: RDD[(String, Double)] = locRDD.reduceByKey(_+_)
        //将数据传入到数据存储的类中去
        /**
          * 第一个参数是初步处理的数据
          * 第二个参数是要处理的业务逻辑的标志
          */
        ExemData2RedisUtil.doDataSave(ans,"local")
    }
}
