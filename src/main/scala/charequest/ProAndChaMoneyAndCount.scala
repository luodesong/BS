package charequest


import org.apache.spark.rdd.RDD
import utils.{Data2MysqlUtil}

/**
  * 充值机构分布
  */
object ProAndChaMoneyAndCount {
    /**
      * 业务处理方法
      *     agrs：是一个不定参数类型的参数列表
      */
    //1)	以省份为维度,统计每小时各省的充值笔数和充值金额
    def getProAndChaMoneyAndMinAndCount(agrs: Any*): Unit = {
        /**
          * 获取第一个参数，将其转化为对应的类型
          */
        //省份，日期，小时，分钟，提交价钱，是否成功，提交一次的标记
        val datasTuple: RDD[(String, String, String, String, Double, Int, Int)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, Double, Int, Int)]]
        //过滤掉出来成功的数据
        val provAndChaAndMinInfo: RDD[(String, String, String, String, Double, Int, Int)] = datasTuple.map(x => x).filter(_._6 == 1)
        //组装需要的数据
        //（（省份，分钟），（提交价钱，一次））
        val provAndChaAndMinRDD: RDD[((String, String), (Double, Int))] = provAndChaAndMinInfo.map(x => {
            ((x._1, x._4), (x._5,1))
        })
        //这一步是预先聚合的效果，能够完成一个批次提前聚合的效果，减少输出数据到数据库时候发生的网络io的次数
        val ans: RDD[((String, String),(Double, Int))] = provAndChaAndMinRDD.reduceByKey((x, y) => {
            (x._1 + y._1, x._2 + y._2)
        })
        //将数据传入到数据存储的类中去
        /**
          * 第一个参数是初步处理的数据
          * 第二个参数是要处理的业务逻辑的标志
          */
        Data2MysqlUtil.doDataSave(ans,"proAndChaMoneyAndMinAndCount")
    }
    //2)	以省份为维度,统计每分钟各省的充值笔数和充值金额
    def getProAndChaMoneyAndHourAndCount(agrs: Any*): Unit = {
        /**
          * 获取第一个参数，将其转化为对应的类型
          */
        //省份，日期，小时，分钟，提交价钱，是否成功，提交一次的标记
        val datasTuple: RDD[(String, String, String, String, Double, Int, Int)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, Double, Int, Int)]]
        //过滤掉出来成功的数据
        val provAndChaAndMinInfo: RDD[(String, String, String, String, Double, Int, Int)] = datasTuple.map(x => x).filter(_._6 == 1)
        //组装需要的数据
        //（（省份，小时），（提交价钱，一次））
        val provAndChaAndMinRDD: RDD[((String, String), (Double, Int))] = provAndChaAndMinInfo.map(x => {
            ((x._1, x._3), (x._5,1))
        })
        //这一步是预先聚合的效果，能够完成一个批次提前聚合的效果，减少输出数据到数据库时候发生的网络io的次数
        val ans: RDD[((String, String),(Double, Int))] = provAndChaAndMinRDD.reduceByKey((x, y) => {
            (x._1 + y._1, x._2 + y._2)
        })
        //将数据传入到数据存储的类中去
        /**
          * 第一个参数是初步处理的数据
          * 第二个参数是要处理的业务逻辑的标志
          */
        Data2MysqlUtil.doDataSave(ans,"proAndChaMoneyAndHourAndCount")
    }
}
