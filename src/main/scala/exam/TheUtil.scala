package exam

object TheUtil {

    def getIpLong(ip:String) : Long = {
        val strings: Array[String] = ip.split("[.]")
        var ipNum = 0L
        for (i <- 0 until strings.length) {
            ipNum = strings(i).toLong | ipNum << 8L
        }
        ipNum
    }
    def binarySearch(ipLong: Long, value: Array[(String, String, String)]) : Int = {
        var start : Int = 0
        var end : Int = value.length - 1
        while (start <= end) {
            var middle = (start + end) / 2
            if (ipLong >= value(middle)._1.toLong && ipLong <= value(middle)._2.toLong) {
                return middle
            } else if (ipLong < value(middle)._1.toLong) {
                end = middle - 1
            } else {
                start = middle + 1
            }
        }
        -1
    }
}
