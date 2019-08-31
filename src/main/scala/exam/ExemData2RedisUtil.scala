package exam

import org.apache.spark.rdd.RDD
import utils.JedisConnectionPool

/**
  * 数据出到数据库中
  */
object ExemData2RedisUtil {
    def doDataSave(args: Any*): Unit = {
        /**
          * 获取第二个参数
          * flag：代表要处理的业务
          */
        val flag: String = args(1).asInstanceOf[String]
        /**
          * 不同的flag值对应一个业务
          */
        if (flag.equals("money")) {
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
                    jedis.hincrBy("exam:order:day:" + t._1, "money", t._2.toLong)
                }
                )
                //关闭对应连接
                JedisConnectionPool.resConnection(jedis)
            })
        }
        if (flag.equals("count")) {
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
                    jedis.hincrBy("exam:order:fac:" + t._1, "count", t._2.toLong)
                }
                )
                //关闭对应连接
                JedisConnectionPool.resConnection(jedis)
            })
        }

        if (flag.equals("local")) {
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
                    jedis.hincrBy("exam:order:local:" + t._1, "money", t._2.toLong)
                }
                )
                //关闭对应连接
                JedisConnectionPool.resConnection(jedis)
            })
        }
    }
}
