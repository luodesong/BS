package busquality

import org.apache.spark.rdd.RDD
import utils.{Data2MysqlUtil, Data2RedisUtil}

/**
  * 2.	业务质量（存入MySQL）
  * 累加器的做法
  */
object FailCount {
    def getAns(agrs: Any*): Unit = {
        //省份，日期，小时，分钟，提交价钱，是否成功，提交一次的标记
        val datasTuple: RDD[(String, String, String, String, Double, Int, Int)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, Double, Int, Int)]]
        val failInfo: RDD[(String, String, String, String, Double, Int, Int)] = datasTuple.map(x => x).filter(_._6 == 0)
        val failInfoTuple: RDD[((String, String), Int)] = failInfo.map(x => {
            ((x._1, x._3), 1)
        })
        val ans: RDD[((String, String), Int)] = failInfoTuple.reduceByKey(_+_)
        Data2MysqlUtil.doDataSave(ans,"failCount")
    }
}

/*
stream.foreachRDD({
            rdd =>
                rdd.foreachPartition({
                    it =>
                        val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                        //获取一个连接
                        val jedis = JedisConnectionPool.getConnection()
                        //获取一个连接（mysql的）
                        val connection: Connection = JdbcConnectionPool.getConn()
                        val statement: Statement = connection.createStatement()
                        // 业务处理
                        val data: List[String] = it.map(_.value()).toList
                        /**
                          * 实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
                          */
                        data.map(x => {
                            //通过这个获取时间
                            val requestId: String = JsonUtil.getString(x, "requestId")
                            //获取省份
                            val provinceCode: String = JsonUtil.getString(x, "provinceCode")
                            //判断数据是不是可以用
                            val serviceName: String = JsonUtil.getString(x, "serviceName")

                            //这个是在exector端执行的 如果直接执行这个的会报一个没有序列化的错
                                if (serviceName.equals("reChargeNotifyReq")) {
                                val proviMap: Map[String, String] = broadcast.value
                                val provi: String = proviMap.getOrElse(provinceCode,"")
                                val cTime: String = requestId.substring(0, 10)
                                //判断是否成功
                                val bussinessRst: String = JsonUtil.getString(x,"bussinessRst")
                                if (!bussinessRst.equals("0000")) {
                                    val sql: String = s"select * from proviAndFail where provinceCode = '${provi}' and cTime = '${cTime}'"
                                    val set: ResultSet = statement.executeQuery(sql)
                                    /**
                                      * set == null || !set.next()
                                      *     不能用null，因为会创建的时候就是一个非空的，必须得用!set.next()判断
                                      */
                                    if (!set.next()) {
                                        val sql1: String = s"insert into proviAndFail values('${provi}','${cTime}', 1)"
                                        statement.execute(sql1)
                                    } else {
                                        val sql2: String = s"update proviAndFail set counts = counts + 1 where provinceCode = '${provi}' and ctime = '${cTime}'"
                                        statement.execute(sql2)
                                    }
                                }
                            }
                        })
                        // 将偏移量进行更新
                        for (or <- offestRange) {
                            jedis.hset("bs:offset:" + groupId, or.topic + "-" + or.partition, or.untilOffset.toString)
                        }
                        JdbcConnectionPool.releaseCon(connection)
                        JedisConnectionPool.resConnection(jedis)
                })
        })
        // 启动
        ssc.start()
        ssc.awaitTermination()
    }
 */