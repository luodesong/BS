import org.junit.Test
import util.JedisConnectionPool

class TestRedis {
    @Test
    def testOne(): Unit = {
        println(JedisConnectionPool.getConnection().get("dmp:business:wk8r7187"))
    }

}
