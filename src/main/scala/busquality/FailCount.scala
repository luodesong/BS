package busquality

import org.apache.spark.rdd.RDD
import utils.{Data2MysqlUtil, Data2RedisUtil}

/**
  * 统计每小时各个省份的充值失败数据量
  * 累加器的做法
  */
object FailCount {
    /**
      * 业务处理方法
      *     agrs：是一个不定参数类型的参数列表
      */
    def getAns(agrs: Any*): Unit = {
        /**
          * 获取第一个参数，将其转化为对应的类型
          */
        //省份，日期，小时，分钟，提交价钱，是否成功，提交一次的标记
        val datasTuple: RDD[(String, String, String, String, Double, Int, Int)] = agrs(0).asInstanceOf[RDD[(String, String, String, String, Double, Int, Int)]]
        //过滤掉出来失败的数据
        val failInfo: RDD[(String, String, String, String, Double, Int, Int)] = datasTuple.map(x => x).filter(_._6 == 0)
        //组装需要的数据
        //（（省份，小时），一次）
        val failInfoTuple: RDD[((String, String), Int)] = failInfo.map(x => {
            ((x._1, x._3), 1)
        })
        //这一步是预先聚合的效果，能够完成一个批次提前聚合的效果，减少输出数据到数据库时候发生的网络io的次数
        val ans: RDD[((String, String), Int)] = failInfoTuple.reduceByKey(_+_)
        //将数据传入到数据存储的类中去
        /**
          * 第一个参数是初步处理的数据
          * 第二个参数是要处理的业务逻辑的标志
          */
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