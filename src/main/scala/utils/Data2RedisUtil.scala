package utils

import org.apache.spark.rdd.RDD

/**
  * 数据出到数据库中
  */
object Data2RedisUtil {
    def doDataSave(args: Any*): Unit = {
        /**
          * 获取第二个参数
          * flag：代表要处理的业务
          */
        val flag: String = args(1).asInstanceOf[String]
        /**
          * 不同的flag值对应一个业务
          */
        if (flag.equals("orderAll")) {
            /**
              * 将获取到的第一个参数转化为本业务要的数据类型
              */
            val ansRDD: RDD[(String, (Double, Int, Int))] = args(0).asInstanceOf[RDD[(String, (Double, Int, Int))]]
            //对每个分区进行操作
            /**
              * 这样做的原因：
              *     1. 序列化连接
              *     2. 减少连接数
              */
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val jedis = JedisConnectionPool.getConnection()
                //循环对每条数据进行操作
                x.foreach(t => {
                    jedis.hincrBy("bs:order:dayall:" + t._1, "orders", t._2._3.toLong)
                    jedis.hincrBy("bs:order:dayall:" + t._1, "success", t._2._2.toLong)
                    jedis.hincrBy("bs:order:dayall:" + t._1, "money", t._2._1.toLong)
                }
                )
                //关闭对应连接
                JedisConnectionPool.resConnection(jedis)
            })
        }

        /**
          * 不同的flag值对应一个业务
          */
        if (flag.equals("orderAndMin")) {
            /**
              * 将获取到的第一个参数转化为本业务要的数据类型
              */
            val ansRDD: RDD[(String, (Double, Int, Int))] = args(0).asInstanceOf[RDD[(String, (Double, Int, Int))]]
            //对每个分区进行操作
            /**
              * 这样做的原因：
              *     1. 序列化连接
              *     2. 减少连接数
              */
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val jedis = JedisConnectionPool.getConnection()
                //循环对每条数据进行操作
                x.foreach(t => {
                    jedis.hincrBy("bs:order:minall:" + t._1, "orders", t._2._3.toLong)
                    jedis.hincrBy("bs:order:minall:" + t._1, "success", t._2._2.toLong)
                    jedis.hincrBy("bs:order:minall:" + t._1, "money", t._2._1.toLong)
                }
                )
                //关闭对应连接
                JedisConnectionPool.resConnection(jedis)
            })
        }

        /**
          * 不同的flag值对应一个业务
          */
        if (flag.equals("dayAndMoney")) {
            /**
              * 将获取到的第一个参数转化为本业务要的数据类型
              */
            val ansRDD: RDD[(String, Double)] = args(0).asInstanceOf[RDD[(String, Double)]]
            //对每个分区进行操作
            /**
              * 这样做的原因：
              *     1. 序列化连接
              *     2. 减少连接数
              */
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val jedis = JedisConnectionPool.getConnection()
                //循环对每条数据进行操作
                x.foreach(t => {
                    jedis.incrByFloat("bs:phone:daymoney:" + t._1, t._2)
                })
                //关闭对应连接
                JedisConnectionPool.resConnection(jedis)
            })
        }

        /**
          * 不同的flag值对应一个业务
          */
        if (flag.equals("monthAndMoneyAvg")) {
            /**
              * 将获取到的第一个参数转化为本业务要的数据类型
              */
            val ansRDD: RDD[((String, String), (Double, Int))] = args(0).asInstanceOf[RDD[((String, String), (Double, Int))]]
            //对每个分区进行操作
            /**
              * 这样做的原因：
              *     1. 序列化连接
              *     2. 减少连接数
              */
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val jedis = JedisConnectionPool.getConnection()
                //循环对每条数据进行操作
                x.foreach(t => {
                    val month: String = jedis.hget("bs:phone:monthmoney:" + t._1._1, t._1._2)
                    if (month == null || month.equals("")) {
                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "money", t._2._1.toString)
                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "count", t._2._2.toString)
                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "avg", (t._2._1 / t._2._2).toString)
                    } else {
                        val money: Double = jedis.hget("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "money").toDouble
                        val count: Long = jedis.hget("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "count").toLong

                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "money", (t._2._1 + money).toString)
                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "count", (t._2._2 + count).toString)
                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "avg", ((t._2._1 + money) / (t._2._2 + count)).toString)
                    }
                })
                //关闭对应连接
                JedisConnectionPool.resConnection(jedis)
            })
        }

    }
}
