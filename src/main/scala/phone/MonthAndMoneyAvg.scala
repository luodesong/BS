package phone

import org.apache.spark.rdd.RDD
import utils.Data2RedisUtil

/**
  * 2.求每月各手机号充值的平均金额
  */
object MonthAndMoneyAvg {
    /**
      * 业务处理方法
      *     agrs：是一个不定参数类型的参数列表
      */
    def getAns(args: Any*): Unit = {
        /**
          * 获取第一个参数，将其转化为对应的类型
          */
        //id, phonenumber,money, date, lat, log
        val logs: RDD[(String, String, Double, String, String, String)] = args(0).asInstanceOf[RDD[(String, String, Double, String, String, String)]]
        //组装需要的数据
        //（phonenumber，date），（money，一次））
        val monthAndMoney: RDD[((String, String), (Double, Int))] = logs.map(x => {
            val dateTime: String = x._4.substring(5, 7)
            ((x._2, dateTime), (x._3, 1))
        })
        //这一步是预先聚合的效果，能够完成一个批次提前聚合的效果，减少输出数据到数据库时候发生的网络io的次数
        val ans: RDD[((String, String), (Double, Int))] = monthAndMoney.reduceByKey((x, y) => {
            (x._1 + y._1, x._2 + y._2)
        })
        //将数据传入到数据存储的类中去
        /**
          * 第一个参数是初步处理的数据
          * 第二个参数是要处理的业务逻辑的标志
          */
        Data2RedisUtil.doDataSave(ans, "monthAndMoneyAvg")
    }
}
