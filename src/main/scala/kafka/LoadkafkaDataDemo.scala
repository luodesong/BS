package kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

object LoadkafkaDataDemo {
    def main(args: Array[String]): Unit = {
        val checkpointDir: String = ""
        val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointDir, ()=> {createContext})

        ssc.start()
        ssc.awaitTermination()

    }

    def createContext: StreamingContext = {
        //创建上下文

        val conf: SparkConf = new SparkConf().setAppName("one").setMaster("local[*]")

        val ssc: StreamingContext = new StreamingContext(conf, Milliseconds(5000))

        //请求kafka的配置信息
        val kafkaParam: Map[String, Object] = Map[String, Object](
            "bootstrap.servers" -> "min1:9092,min2:9092,min3:9092"
            ,
            //指定反序列化方式
            "key.deserializer" -> classOf[StringDeserializer]
            ,
            "value.deserializer" -> classOf[StringDeserializer]
            ,

            //指定组
            "group.id" -> "group01"
            ,
            //指定消费位置
            "auto.offset.reset" -> "latest"
            ,
            //如果value合法，自动提交offset
            "enable.auto.commit" -> (true:java.lang.Boolean)
        )

        val topics = Array("test1")
        //消费数据
        val logs: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            //订阅消费信息
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParam)
        )

        //开始操作数据并且观察数据结构
        logs.foreachRDD(rdd => {
            //获取数据对应的offset的范围
            val offsetList: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

            rdd.foreachPartition(part => {
                part.foreach(line => {
                    val offsetInfo: OffsetRange = offsetList(TaskContext.get().partitionId())
                    println("topicPartition:" + offsetInfo.topicPartition())
                    println("topic:" + offsetInfo.topic)
                    println("fromoffset:" + offsetInfo.fromOffset)

                })
            })
        })
        ssc
    }

}
