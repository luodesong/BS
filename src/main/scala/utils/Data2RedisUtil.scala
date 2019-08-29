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
                    jedis.hincrBy("bs:order:minall:" + t._1, "orders", t._2._3.toLong)
                    jedis.hincrBy("bs:order:minall:" + t._1, "success", t._2._2.toLong)
                    jedis.hincrBy("bs:order:minall:" + t._1, "money", t._2._1.toLong)
                }
                )
                JedisConnectionPool.resConnection(jedis)
            })
        }

        if (flag.equals("dayAndMoney")) {
            val ansRDD: RDD[(String, Double)] = args(0).asInstanceOf[RDD[(String, Double)]]
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val jedis = JedisConnectionPool.getConnection()
                x.foreach(t => {

                    jedis.incrByFloat("bs:phone:daymoney:" + t._1, t._2)
                })
                JedisConnectionPool.resConnection(jedis)
            })
        }

        if (flag.equals("monthAndMoneyAvg")) {
            val ansRDD: RDD[((String, String), (Double, Int))] = args(0).asInstanceOf[RDD[((String, String), (Double, Int))]]
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val jedis = JedisConnectionPool.getConnection()
                x.foreach(t => {
                    val month: String = jedis.hget("bs:phone:monthmoney:" + t._1._1, t._1._2)
                    if (month == null || month.equals("")) {
                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "money", t._2._1.toString)
                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "count", t._2._2.toString)
                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "avg", (t._2._1 / t._2._2).toString)
                    } else {
                        val money: Double = jedis.hget("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "money").toDouble
                        val count: Long = jedis.hget("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2,"count").toLong

                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "money", (t._2._1 + money).toString)
                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "count", (t._2._2 + count).toString)
                        jedis.hset("bs:phone:monthmoney:" + t._1._1 + ":" + t._1._2, "avg", ((t._2._1 + money) / (t._2._2 + count)).toString)
                    }
                })
                JedisConnectionPool.resConnection(jedis)
            })
        }

    }
}
