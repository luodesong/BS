package busoverview

import java.lang

import charequest.ProAndChaMoneyAndCount
import offeset.JedisOffset
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{JedisConnectionPool, JsonUtil}

object MySparkContext {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
                .setAppName("offset")
                .setMaster("local[2]")
                // 设置每秒钟每个分区拉取kafka的速率
                .set("spark.streaming.kafka.maxRatePerPartition", "1000")
                // 设置序列化机制
                .set("spark.serlizer", "org.apache.spark.serializer.KryoSerializer")

        //批次处理设置了为3秒钟
        val ssc = new StreamingContext(conf, Seconds(3))
        val sc = ssc.sparkContext

        //设置广播变量
        val citys: RDD[String] = sc.textFile("E:\\Test-workspace\\testSpark\\input\\project\\BS\\citys")
        val coredAndCity: RDD[(String, String)] = citys.map(x => {
            val strings: Array[String] = x.split(" ")
            (strings(0), strings(1))
        })
        val broadcast: Broadcast[Map[String, String]] = sc.broadcast(coredAndCity.collect.toMap)

        // 配置参数
        // 配置基本参数
        // 组名
        val groupId = "luodesong"
        // topic
        val topics: Array[String] = Array("dataForBs")
        // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
        val brokerList = "min1:9092,min2:9092,min3:9092"
        // 编写Kafka的配置参数
        val kafkas = Map[String, Object](
            "bootstrap.servers" -> brokerList,
            // kafka的Key和values解码方式
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupId,
            // 从头消费
            "auto.offset.reset" -> "earliest",
            // 不需要程序自动提交Offset
            "enable.auto.commit" -> (false: lang.Boolean)
        )

        // 第一步获取Offset
        // 第二步通过Offset获取Kafka数据
        // 第三步提交更新Offset
        // 获取Offset
        var fromOffset: Map[TopicPartition, Long] = JedisOffset(groupId)
        // 判断一下有没数据
        val stream: InputDStream[ConsumerRecord[String, String]] =
            if (fromOffset.size == 0) {
                KafkaUtils.createDirectStream(ssc,
                    // 本地策略
                    // 将数据均匀的分配到各个Executor上面
                    LocationStrategies.PreferConsistent,
                    // 消费者策略
                    // 可以动态增加分区
                    ConsumerStrategies.Subscribe[String, String](topics, kafkas)
                )
            } else {
                // 不是第一次消费
                KafkaUtils.createDirectStream(
                    ssc,
                    LocationStrategies.PreferConsistent,
                    ConsumerStrategies.Assign[String, String](fromOffset.keys, kafkas, fromOffset)
                )
            }
        stream.foreachRDD({
            rdd =>
                val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                //获取一个连接
                val jedis = JedisConnectionPool.getConnection()
                // 业务处理
                val data: RDD[String] = rdd.map(_.value()).filter(_.contains("reChargeNotifyReq"))
                val datasTuple: RDD[(String, String, String, String, Double, Int, Int)] = data.map(x => {
                    //对每条数据进行json解析
                    //判断是否成功
                    val bussinessRst: String = JsonUtil.getString(x, "bussinessRst")
                    val success: Int = if (bussinessRst.equals("0000")) 1 else 0
                    //充值金额
                    val chMoney: Double = if (bussinessRst.equals("0000")) JsonUtil.getString(x, "chargefee").toDouble else 0.0
                    //开始时间(天)
                    val startTimeDay: String = JsonUtil.getString(x, "requestId").substring(0, 8)
                    //开始时间(每小时)
                    val startTimeHour: String = JsonUtil.getString(x, "requestId").substring(0, 10)
                    //开始时间(每小时)
                    val startTimeMin: String = JsonUtil.getString(x, "requestId").substring(0, 12)
                    //操作的城市
                    //获取省份
                    val provinceCode: String = JsonUtil.getString(x, "provinceCode")
                    val proviMap: Map[String, String] = broadcast.value
                    val provi: String = proviMap.getOrElse(provinceCode, "")

                    (provi, startTimeDay, startTimeHour, startTimeMin, chMoney, success, 1)
                })

                /**
                  * 1)	统计全网的充值订单量, 充值金额, 充值成功数
                  */
                //OrderAll.getAns(datasTuple)

                /**
                  * 实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
                  */
                //OrderAndMin.getAns(datasTuple)

                //FailCount.getAns(datasTuple)
                //ChaAndDist.getAns(datasTuple)
                ProAndChaMoneyAndCount.getProAndChaMoneyAndHourAndCount(datasTuple)
                //ProAndChaMoneyAndCount.getProAndChaMoneyAndMinAndCount(datasTuple)

                // 将偏移量进行更新
                for (or <- offestRange) {
                    jedis.hset("bs:offset:" + groupId, or.topic + "-" + or.partition, or.untilOffset.toString)
                }
                JedisConnectionPool.resConnection(jedis)
        })
        // 启动
        ssc.start()
        ssc.awaitTermination()
    }
}


/*
data.map(x => {
    //对每条数据进行json解析
    val serviceName: String = JsonUtil.getString(x, "serviceName")
    val chargefee: String = JsonUtil.getString(x, "chargefee")
    val bussinessRst: String = JsonUtil.getString(x, "bussinessRst")
    if (serviceName.equals("reChargeNotifyReq")) {
        val orderAll: util.Map[String, String] = jedis.hgetAll("bs:order:orderAll")
        if (orderAll == null || orderAll.isEmpty) {
            jedis.hset("bs:order:orderAll", "orders", "0")
            jedis.hset("bs:order:orderAll", "money", "0.0")
            jedis.hset("bs:order:orderAll", "success", "0")
        } else {
            val orders: Int = jedis.hget("bs:order:orderAll", "orders").toInt + 1
            val money: Double = jedis.hget("bs:order:orderAll", "money").toDouble + chargefee.toDouble
            var success: Int = jedis.hget("bs:order:orderAll", "success").toInt
            if (bussinessRst.equals("0000")) {
                success = success + 1
            }
            jedis.hset("bs:order:orderAll", "orders", orders.toString)
            jedis.hset("bs:order:orderAll", "money", money.toString)
            jedis.hset("bs:order:orderAll", "success", success.toString)
        }
    }
})

 */

/*
data.map(x => {
    //通过这个获取时间
    val requestId: String = JsonUtil.getString(x, "requestId")
    //判断数据是不是可以用
    val serviceName: String = JsonUtil.getString(x, "serviceName")
    if (serviceName.equals("reChargeNotifyReq")) {
        val str: String = requestId.substring(0, 12)
        //val l: Long = MyDateUtil.getTime(str)
        //val startTime: String = MyDateUtil.getTimeString(l-60000)
        val lastMiOr: String = jedis.get("bs:order:time:" + str)
        if (lastMiOr == null || lastMiOr.isEmpty) {
            jedis.set("bs:order:time:" + str, "1")
        } else {
            val key: Int = jedis.get("bs:order:time:" + str).toInt + 1
            jedis.set("bs:order:time:" + str, key.toString)
        }
    }
})
 */