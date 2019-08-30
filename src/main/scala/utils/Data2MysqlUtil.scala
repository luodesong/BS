package utils

import java.sql.{Connection, ResultSet, Statement}

import org.apache.spark.rdd.RDD

/**
  * 将数据插入数据库的方法
  */
object Data2MysqlUtil {

    def doDataSave(args: Any*): Unit = {
        /**
          * 获取第二个参数
          * flag：代表要处理的业务
          */
        val flag: String = args(1).asInstanceOf[String]

        /**
          * 不同的flag值对应一个业务
          */
        if (flag.equals("failCount")) {
            /**
              * 将获取到的第一个参数转化为本业务要的数据类型
              */
            val ansRDD: RDD[((String, String), Int)] = args(0).asInstanceOf[RDD[((String, String), Int)]]
            //对每个分区进行操作
            /**
              * 这样做的原因：
              *     1. 序列化连接
              *     2. 减少连接数
              */
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val connection: Connection = JdbcConnectionPool.getConn()
                //循环对每条数据进行操作
                x.foreach(
                    tuple => {
                        val statement: Statement = connection.createStatement()
                        val sql: String = s"select * from proviAndFail where provinceCode = '${tuple._1._1}' and cTime = '${tuple._1._2}'"
                        //查找结果集
                        val set: ResultSet = statement.executeQuery(sql)

                        /**
                          * set == null || !set.next()
                          * 不能用null，因为会创建的时候就是一个非空的，必须得用!set.next()判断
                          */
                        if (!set.next()) {
                            /**
                              * 添加一条数据
                              * 添加的是每个分区处理好的数据
                              */
                            val sql1: String = s"insert into proviAndFail values('${tuple._1._1}','${tuple._1._2}', ${tuple._2})"
                            statement.execute(sql1)
                        } else {
                            /**
                              * 更新一条数据
                              */
                            val sql2: String = s"update proviAndFail set counts = counts + ${tuple._2} where provinceCode = '${tuple._1._1}' and ctime = '${tuple._1._2}'"
                            statement.execute(sql2)
                        }

                    }
                )
                //关闭对应连接
                JdbcConnectionPool.releaseCon(connection)
            })
        }

        /**
          * 不同的flag值对应一个业务
          */
        if (flag.equals("chaAndDist")) {
            /**
              * 将获取到的第一个参数转化为本业务要的数据类型
              */
            val ansRDD: RDD[(String, (Double, Int))] = args(0).asInstanceOf[RDD[(String, (Double, Int))]]
            //对每个分区进行操作
            /**
              * 这样做的原因：
              *     1. 序列化连接
              *     2. 减少连接数
              */
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val connection: Connection = JdbcConnectionPool.getConn()
                //循环对每条数据进行操作
                x.foreach(
                    tuples => {
                        val statement: Statement = connection.createStatement()
                        val sql: String = s"select * from hourMoney where chTime = '${tuples._1}'"
                        //查找结果集
                        val set: ResultSet = statement.executeQuery(sql)

                        /**
                          * set == null || !set.next()
                          * 不能用null，因为会创建的时候就是一个非空的，必须得用!set.next()判断
                          */
                        if (!set.next()) {
                            /**
                              * 添加一条数据
                              * 添加的是每个分区处理好的数据
                              */
                            val sql1: String = s"insert into hourMoney values('${tuples._1}',${tuples._2._1.toDouble}, ${tuples._2._2})"
                            statement.execute(sql1)
                        } else {
                            /**
                              * 更新一条数据
                              */
                            val sql2: String = s"update hourMoney set chargefee = chargefee + ${tuples._2._1.toDouble}, count = count + ${tuples._2._2}  where chTime = '${tuples._1}'"
                            statement.execute(sql2)
                        }

                    }
                )
                //关闭对应连接
                JdbcConnectionPool.releaseCon(connection)
            })
        }

        /**
          * 不同的flag值对应一个业务
          */
        if (flag.equals("proAndChaMoneyAndMinAndCount")) {
            /**
              * 将获取到的第一个参数转化为本业务要的数据类型
              */
            val ansRDD: RDD[((String, String), (Double, Int))] = args(0).asInstanceOf[RDD[((String, String), (Double, Int))]]
            //对每个分区进行操作
            /**
              * 这样做的原因：
              *     1. 序列化连接
              *     2. 减少连接数
              */
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val connection: Connection = JdbcConnectionPool.getConn()
                //循环对每条数据进行操作
                x.foreach(
                    tuples => {
                        val statement: Statement = connection.createStatement()
                        val sql: String = s"select * from proAndMonAndCountMin where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                        //查找结果集
                        val set: ResultSet = statement.executeQuery(sql)
                        /**
                          * set == null || !set.next()
                          * 不能用null，因为会创建的时候就是一个非空的，必须得用!set.next()判断
                          */
                        if (!set.next()) {
                            /**
                              * 添加一条数据
                              * 添加的是每个分区处理好的数据
                              */
                            val sql1: String = s"insert into proAndMonAndCountMin values('${tuples._1._1}','${tuples._1._2}', ${tuples._2._1}, ${tuples._2._2})"
                            statement.execute(sql1)
                        } else {
                            /**
                              * 更新一条数据
                              */
                            val sql2: String = s"update proAndMonAndCountMin set chargefee = chargefee + ${tuples._2._1}, count = count + ${tuples._2._2} where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                            statement.execute(sql2)
                        }
                    }
                )
                //关闭对应连接
                JdbcConnectionPool.releaseCon(connection)
            })
        }

        /**
          * 不同的flag值对应一个业务
          */
        if (flag.equals("proAndChaMoneyAndHourAndCount")) {
            /**
              * 将获取到的第一个参数转化为本业务要的数据类型
              */
            val ansRDD: RDD[((String, String), (Double, Int))] = args(0).asInstanceOf[RDD[((String, String), (Double, Int))]]
            //对每个分区进行操作
            /**
              * 这样做的原因：
              *     1. 序列化连接
              *     2. 减少连接数
              */
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val connection: Connection = JdbcConnectionPool.getConn()
                //循环对每条数据进行操作
                x.foreach(
                    tuples => {
                        val statement: Statement = connection.createStatement()
                        val sql: String = s"select * from proAndMonAndCountHour where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                        //查找结果集
                        val set: ResultSet = statement.executeQuery(sql)

                        /**
                          * set == null || !set.next()
                          * 不能用null，因为会创建的时候就是一个非空的，必须得用!set.next()判断
                          */
                        if (!set.next()) {
                            /**
                              * 添加一条数据
                              * 添加的是每个分区处理好的数据
                              */
                            val sql1: String = s"insert into proAndMonAndCountHour values('${tuples._1._1}','${tuples._1._2}', ${tuples._2._1}, ${tuples._2._2})"
                            statement.execute(sql1)
                        } else {
                            /**
                              * 更新一条数据
                              */
                            val sql2: String = s"update proAndMonAndCountHour set chargefee = chargefee + ${tuples._2._1}, count = count + ${tuples._2._2} where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                            statement.execute(sql2)
                        }
                    }
                )
                JdbcConnectionPool.releaseCon(connection)
            })
        }
        /**
          * 不同的flag值对应一个业务
          */
        if (flag.equals("proAndChaMoneyAndHourAndCount")) {
            /**
              * 将获取到的第一个参数转化为本业务要的数据类型
              */
            val ansRDD: RDD[((String, String), (Double, Int))] = args(0).asInstanceOf[RDD[((String, String), (Double, Int))]]
            //对每个分区进行操作
            /**
              * 这样做的原因：
              *     1. 序列化连接
              *     2. 减少连接数
              */
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val connection: Connection = JdbcConnectionPool.getConn()
                //循环对每条数据进行操作
                x.foreach(
                    tuples => {
                        val statement: Statement = connection.createStatement()
                        val sql: String = s"select * from proAndMonAndCountHour where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                        //查找结果集
                        val set: ResultSet = statement.executeQuery(sql)
                        /**
                          * set == null || !set.next()
                          * 不能用null，因为会创建的时候就是一个非空的，必须得用!set.next()判断
                          */
                        if (!set.next()) {
                            /**
                              * 添加一条数据
                              * 添加的是每个分区处理好的数据
                              */
                            val sql1: String = s"insert into proAndMonAndCountHour values('${tuples._1._1}','${tuples._1._2}', ${tuples._2._1}, ${tuples._2._2})"
                            statement.execute(sql1)
                        } else {
                            /**
                              * 更新一条数据
                              */
                            val sql2: String = s"update proAndMonAndCountHour set chargefee = chargefee + ${tuples._2._1}, count = count + ${tuples._2._2} where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                            statement.execute(sql2)
                        }
                    }
                )
                //关闭对应连接
                JdbcConnectionPool.releaseCon(connection)
            })
        }
    }
}
