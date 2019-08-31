package exam

import java.lang
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{JedisConnectionPool, JsonUtil}

/**
  * Receiver方式；当一个任务从driver发送到executor执行的时候，这时候，将数据拉取到executor中做操作，但是如果数据太大的话，这时候不能全放在内存中，receiver通过WAL，设置了本地存储，他会存放本地，保证数据不丢失，然后使用Kafka高级API通过zk来维护偏移量，保证数据的衔接性，其实可以说，receiver数据在zk获取的，这种方式效率低，而且极易容易出现数据丢失
  *
  * Direct 方式； 他使用Kafka底层Api 并且消费者直接连接kafka的分区上，因为createDirectStream创建的DirectKafkaInputDStream每个batch所对应的RDD的分区与kafka分区一一对应，但是需要自己维护偏移量，迭代计算，即用即取即丢，不会给内存造成太大的压力，这样效率很高
  *
  *
  * 该文件是一个电商网站某一天用户购买商品的订单成交数据，每一行有多个字段，用空格分割，字段的含义如下
  * 时间   				ip地址          商品分类   购买明细     商品金额
  * 201908302100       202.106.196.115 	手机       iPhone8      8000
  * 问题1.计算出总的成交量总额（结果保存到Redis中）
  * 问题2.计算每个商品分类的成交量的（结果保存到Redis中）
  * 问题3.计算每个地域的商品成交总金额（结果保存到Redis中）
  *
  * 201908302223 202.106.196.115 手机 iPhone8 8000
  * 201908301623 202.106.0.20 服装 布莱奥尼西服 199
  * 201908301823 202.102.152.3 家具 婴儿床 2000
  * 201908302100 202.96.96.68 家电 电饭锅 1000
  * 201908301123 202.98.0.68 化妆品 迪奥香水 200
  * 201908301223 202.96.75.68 食品 奶粉 600
  * 201908301423 202.97.229.133 图书 Hadoop编程指南 90
  * 201908300423 202.106.196.115 手机 手机壳 200
  * 201908302023 202.106.0.20 手机 iPhone8 8000
  * 201908300823 202.102.152.3 家具 婴儿车 2000
  * 201908300223 202.96.96.68 家具 婴儿车 1000
  * 201908300523 202.98.0.68 化妆品 迪奥香水 200
  * 201908300723 202.96.75.68 食品 婴儿床 600
  * 201908300923 202.97.229.133 图书 spark实战 80
  *
  *
  *
  *
  *
  */
object TheContext {
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
        val ipInfoFile: RDD[String] = sc.textFile("E:\\Test-workspace\\testSpark\\input\\day11\\ipsearch\\ip.txt")
        val ipInfo: RDD[(String, String, String)] = ipInfoFile.map(x => {
            val strings: Array[String] = x.split("\\|")
            val start: String = strings(2)
            val end: String = strings(3)
            val city: String = strings(6)
            (start, end, city)
        })

        val broadcast: Broadcast[Array[(String, String, String)]] = sc.broadcast(ipInfo.collect)
        // 配置参数
        // 配置基本参数
        // 组名
        val groupId = "luodesong"
        // topic
        val topics: Array[String] = Array("exam")
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
        var fromOffset: Map[TopicPartition, Long] = ExamJedisOffset(groupId)
        // 判断一下有没数据
        val stream: InputDStream[ConsumerRecord[String, String]] =
            if (fromOffset.size == 0) {
                KafkaUtils.createDirectStream(
                    ssc,
                    // 2、DirectKafkaInputDStream
                    // LocationStrategies:本地策略。为提升性能,可指定Kafka Topic Partition的消费者所在的Executor。
                    // LocationStrategies.PreferConsistent:一致性策略。一般情况下用这个策略就OK。将分区尽可能分配给所有可用Executor。
                    // LocationStrategies.PreferBrokers:特殊情况,如果Executor和Kafka Broker在同一主机,则可使用此策略。
                    // LocationStrategies.PreferFixed:特殊情况,当Kafka Topic Partition负荷倾斜,可用此策略,手动指定Executor来消费特定的Partition.
                    // ConsumerStrategies:消费策略。
                    // ConsumerStrategies.Subscribe/SubscribePattern:可订阅一类Topic,且当新Topic加入时，会自动订阅。一般情况下，用这个就OK。
                    // ConsumerStrategies.Assign:可指定要消费的Topic-Partition,以及从指定Offset开始消费。

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

                    /**
                      * @Experimental
                      * def Assign[K, V](
                      * topicPartitions: Iterable[TopicPartition],
                      * kafkaParams: collection.Map[String, Object],
                      * offsets: collection.Map[TopicPartition, Long]): ConsumerStrategy[K, V] = {
                      * new Assign[K, V](
                      * new ju.ArrayList(topicPartitions.asJavaCollection),
                      * new ju.HashMap[String, Object](kafkaParams.asJava),
                      * new ju.HashMap[TopicPartition, jl.Long](offsets.mapValues(l => new jl.Long(l)).asJava))
                      * }
                      *
                      */
                    ConsumerStrategies.Assign[String, String](fromOffset.keys, kafkas, fromOffset)
                )
            }
        stream.foreachRDD({
            rdd =>

                /**
                  * Represents a range of offsets from a single Kafka TopicPartition. Instances of this class
                  * can be created with `OffsetRange.create()`.
                  * OffsetRange里面存的是以下这四个值
                  * topic： Kafka topic name
                  * partition： Kafka partition id
                  * fromOffset： Inclusive starting offset
                  * untilOffset： Exclusive ending offset
                  */
                val offestRange: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                //获取一个连接
                val jedis = JedisConnectionPool.getConnection()
                // 业务处理
                val data: RDD[String] = rdd.map(_.value())
                val logs: RDD[(String, String, String, String, String)] = data.map(x => {
                    val strings: Array[String] = x.split(" ")
                    val times: String = strings(0)
                    val theIp: String = strings(1)
                    val frea: String = strings(2)
                    val descP: String = strings(3)
                    val money: String = strings(4)
                    val ip: Long = TheUtil.getIpLong(theIp)
                    val index: Int = TheUtil.binarySearch(ip, broadcast.value)
                    val tuple: (String, String, String) = broadcast.value(index)
                    (times, tuple._3, frea, descP, money)
                })
                ExamStream.getAns(logs)
                ExamStream.getAns01(logs)
                ExamStream.getAns02(logs)
                // 将偏移量进行更新
                for (or <- offestRange) {
                    jedis.hset("exam:offset:" + groupId, or.topic + "-" + or.partition, or.untilOffset.toString)
                }
                JedisConnectionPool.resConnection(jedis)
        })
        // 启动
        ssc.start()
        ssc.awaitTermination()
    }
}
