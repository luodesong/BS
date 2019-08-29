package utils

import java.sql.{Connection, ResultSet, Statement}

import org.apache.spark.rdd.RDD

object Data2MysqlUtil {

    def doDataSave(args: Any*): Unit = {
        val flag: String = args(1).asInstanceOf[String]
        if (flag.equals("failCount")) {
            val ansRDD: RDD[((String, String), Int)] = args(0).asInstanceOf[RDD[((String, String), Int)]]
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val connection: Connection = JdbcConnectionPool.getConn()
                x.foreach(
                    tuple => {
                        val statement: Statement = connection.createStatement()
                        /**
                          * 实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
                          */
                        val sql: String = s"select * from proviAndFail where provinceCode = '${tuple._1._1}' and cTime = '${tuple._1._2}'"
                        val set: ResultSet = statement.executeQuery(sql)
                        /**
                          * set == null || !set.next()
                          * 不能用null，因为会创建的时候就是一个非空的，必须得用!set.next()判断
                          */
                        if (!set.next()) {
                            val sql1: String = s"insert into proviAndFail values('${tuple._1._1}','${tuple._1._2}', ${tuple._2})"
                            statement.execute(sql1)
                        } else {
                            val sql2: String = s"update proviAndFail set counts = counts + ${tuple._2} where provinceCode = '${tuple._1._1}' and ctime = '${tuple._1._2}'"
                            statement.execute(sql2)
                        }

                    }
                )
                JdbcConnectionPool.releaseCon(connection)
            })
        }
        if (flag.equals("chaAndDist")) {
            val ansRDD: RDD[(String, (Double, Int))] = args(0).asInstanceOf[RDD[(String, (Double, Int))]]
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val connection: Connection = JdbcConnectionPool.getConn()
                x.foreach(
                    tuples => {
                        val statement: Statement = connection.createStatement()
                        /**
                          * 实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
                          */
                        val sql: String = s"select * from hourMoney where chTime = '${tuples._1}'"
                        val set: ResultSet = statement.executeQuery(sql)
                        /**
                          * set == null || !set.next()
                          *     不能用null，因为会创建的时候就是一个非空的，必须得用!set.next()判断
                          */
                        if (!set.next()) {
                            val sql1: String = s"insert into hourMoney values('${tuples._1}',${tuples._2._1.toDouble}, ${tuples._2._2})"
                            statement.execute(sql1)
                        } else {
                            val sql2: String = s"update hourMoney set chargefee = chargefee + ${tuples._2._1.toDouble}, count = count + ${tuples._2._2}  where chTime = '${tuples._1}'"
                            statement.execute(sql2)
                        }

                    }
                )
                JdbcConnectionPool.releaseCon(connection)
            })
        }

        if (flag.equals("proAndChaMoneyAndMinAndCount")) {
            val ansRDD: RDD[((String, String), (Double, Int))] = args(0).asInstanceOf[RDD[((String, String), (Double, Int))]]
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val connection: Connection = JdbcConnectionPool.getConn()
                x.foreach(
                    tuples => {
                        val statement: Statement = connection.createStatement()
                        val sql: String = s"select * from proAndMonAndCountMin where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                        val set: ResultSet = statement.executeQuery(sql)
                        /**
                          * set == null || !set.next()
                          *     不能用null，因为会创建的时候就是一个非空的，必须得用!set.next()判断
                          */
                        if (!set.next()) {
                            val sql1: String = s"insert into proAndMonAndCountMin values('${tuples._1._1}','${tuples._1._2}', ${tuples._2._1}, ${tuples._2._2})"
                            statement.execute(sql1)
                        } else {
                            val sql2: String = s"update proAndMonAndCountMin set chargefee = chargefee + ${tuples._2._1}, count = count + ${tuples._2._2} where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                            statement.execute(sql2)
                        }
                    }
                )
                JdbcConnectionPool.releaseCon(connection)
            })
        }

        if (flag.equals("proAndChaMoneyAndHourAndCount")) {
            val ansRDD: RDD[((String, String), (Double, Int))] = args(0).asInstanceOf[RDD[((String, String), (Double, Int))]]
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val connection: Connection = JdbcConnectionPool.getConn()
                x.foreach(
                    tuples => {
                        val statement: Statement = connection.createStatement()
                        val sql: String = s"select * from proAndMonAndCountHour where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                        val set: ResultSet = statement.executeQuery(sql)
                        /**
                          * set == null || !set.next()
                          *     不能用null，因为会创建的时候就是一个非空的，必须得用!set.next()判断
                          */
                        if (!set.next()) {
                            val sql1: String = s"insert into proAndMonAndCountHour values('${tuples._1._1}','${tuples._1._2}', ${tuples._2._1}, ${tuples._2._2})"
                            statement.execute(sql1)
                        } else {
                            val sql2: String = s"update proAndMonAndCountHour set chargefee = chargefee + ${tuples._2._1}, count = count + ${tuples._2._2} where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                            statement.execute(sql2)
                        }
                    }
                )
                JdbcConnectionPool.releaseCon(connection)
            })
        }

        if (flag.equals("proAndChaMoneyAndHourAndCount")) {
            val ansRDD: RDD[((String, String), (Double, Int))] = args(0).asInstanceOf[RDD[((String, String), (Double, Int))]]
            ansRDD.foreachPartition(x => {
                //获取一个连接
                val connection: Connection = JdbcConnectionPool.getConn()
                x.foreach(
                    tuples => {
                        val statement: Statement = connection.createStatement()
                        val sql: String = s"select * from proAndMonAndCountHour where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                        val set: ResultSet = statement.executeQuery(sql)
                        /**
                          * set == null || !set.next()
                          *     不能用null，因为会创建的时候就是一个非空的，必须得用!set.next()判断
                          */
                        if (!set.next()) {
                            val sql1: String = s"insert into proAndMonAndCountHour values('${tuples._1._1}','${tuples._1._2}', ${tuples._2._1}, ${tuples._2._2})"
                            statement.execute(sql1)
                        } else {
                            val sql2: String = s"update proAndMonAndCountHour set chargefee = chargefee + ${tuples._2._1}, count = count + ${tuples._2._2} where provi = '${tuples._1._1}' and cTime = '${tuples._1._2}'"
                            statement.execute(sql2)
                        }
                    }
                )
                JdbcConnectionPool.releaseCon(connection)
            })
        }
    }
}
