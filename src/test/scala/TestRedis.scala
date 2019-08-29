import org.junit.Test
import utils.JedisConnectionPool

class TestRedis {
    @Test
    def testOne(): Unit = {
        println(JedisConnectionPool.getConnection().get("dmp:business:wk8r7187"))
    }

}
