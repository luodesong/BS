package busoverview


import _root_.utils.{Data2RedisUtil}
import org.apache.spark.rdd.RDD

/**
  * 1)	统计全网的充值订单量, 充值金额, 充值成功
  */
object OrderAll {

    /**
      * 业务处理方法
      *     agrs：是一个不定参数类型的参数列表
      */
    def getAns (agrs: Any*): Unit = {
        /**
          * 获取第一个参数，将其转化为对应的类型
          */
        //省份，日期，小时，分钟，提交价钱，是否成功，提交一次的标记
        val datasTuple: RDD[(String, String, String, String, Double, Int, Int)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, Double, Int, Int)]]

        //组装需要的数据
        //（日期，（提交价钱，是否成功，提交一次的标记）
        val dayAndOrdersRDD: RDD[(String, (Double, Int, Int))] = datasTuple.map(x => {
            (x._2, (x._5, x._6, x._7))
        })

        //这一步是预先聚合的效果，能够完成一个批次提前聚合的效果，减少输出数据到数据库时候发生的网络io的次数
        val dayAndOrders: RDD[(String, (Double, Int, Int))] = dayAndOrdersRDD.reduceByKey((x, y) => {
            (x._1 + y._1, x._2 + y._2, x._3 + y._3)
        })
        //将数据传入到数据存储的类中去
        /**
          * 第一个参数是初步处理的数据
          * 第二个参数是要处理的业务逻辑的标志
          */
        Data2RedisUtil.doDataSave(dayAndOrders, "orderAll")
    }
}
