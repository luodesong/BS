package busoverview

import java.lang

import offeset.JedisOffset
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import phone.MonthAndMoneyAvg
import utils.{JedisConnectionPool, JsonUtil}

object MySparkContext2 {
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


        // 配置参数
        // 配置基本参数
        // 组名
        val groupId = "luodesong07"
        // topic
        val topics: Array[String] = Array("phoneLogsJson")
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
                val data: RDD[String] = rdd.map(_.value())
                val dataTuples: RDD[(String, String, Double, String, String, String)] = data.map(x => {
                    //对每条数据进行json解析
                    //获取id
                    val openid: String = JsonUtil.getString(x, "openid")
                    //获取电话号码
                    val phoneNum: String = JsonUtil.getString(x, "phoneNum")
                    //获取充值的钱数
                    val money: Double = JsonUtil.getString(x, "money").toDouble
                    //获取时间
                    val dates: String = JsonUtil.getString(x, "date")
                    //获取经纬度
                    val lat: String = JsonUtil.getString(x, "lat")
                    val log: String = JsonUtil.getString(x, "log")

                    (openid, phoneNum, money, dates, lat, log)
                })
                //DayAndMoney.getAns(dataTuples)

                MonthAndMoneyAvg.getAns(dataTuples)

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
