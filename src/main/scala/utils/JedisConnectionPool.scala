package utils

import java.util.LinkedList

import com.typesafe.config.ConfigFactory
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * 连接池
  */
object JedisConnectionPool {
    //创建一个连接池配置文件对象对象
    private val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig

    //读取.properties配置文件
    private val config = ConfigFactory.load("redisPool.properties")
    private val max_connection = config.getString("redis.max_connection") //连接池总数
    private val unuse_num = config.getString("redis.unuse_num") //最大空闲数
    private val node : String = config.getString("redis.node")//获取节点名称
    private val port : String = config.getString("redis.port")//获取节点端口

    // 最大连接
    jedisPoolConfig.setMaxTotal(max_connection.toInt)
    // 最大空闲
    jedisPoolConfig.setMaxIdle(unuse_num.toInt)

    //通过配置文件获连接池
    val pool = new JedisPool(jedisPoolConfig, node, port.toInt, 10000)

    // 获取Jedis对象
    def getConnection(): Jedis = {
        pool.getResource
    }

    // 释放连接
    def resConnection(jedis:Jedis): Unit ={
        jedis.close()
    }
}
