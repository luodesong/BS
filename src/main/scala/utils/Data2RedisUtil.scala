package utils

import org.apache.spark.rdd.RDD

object Data2RedisUtil {
    def doDataSave(args: Any*): Unit = {
        val flag: String = args(1).asInstanceOf[String]
        if (flag.equals("orderAll")) {
            val ansRDD: RDD[(String, (Double, Int, Int))] = args(0).asInstanceOf[RDD[(String, (Double, Int, Int))]]
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val jedis = JedisConnectionPool.getConnection()
                x.foreach(t => {
                        jedis.hincrBy("bs:order:dayall:" + t._1, "orders", t._2._3.toLong)
                        jedis.hincrBy("bs:order:dayall:" + t._1, "success", t._2._2.toLong)
                        jedis.hincrBy("bs:order:dayall:" + t._1, "money", t._2._1.toLong)
                    }
                )
                JedisConnectionPool.resConnection(jedis)
            })
        }
        if (flag.equals("orderAndMin")) {
            val ansRDD: RDD[(String, (Double, Int, Int))] = args(0).asInstanceOf[RDD[(String, (Double, Int, Int))]]
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val jedis = JedisConnectionPool.getConnection()
                x.foreach(t => {
                        jedis.hincrBy("bs:order:minall:" +  t._1, "orders", t._2._3.toLong)
                        jedis.hincrBy("bs:order:minall:" +  t._1, "success", t._2._2.toLong)
                        jedis.hincrBy("bs:order:minall:" +  t._1, "money", t._2._1.toLong)
                    }
                )
                JedisConnectionPool.resConnection(jedis)
            })
        }
    }
}
