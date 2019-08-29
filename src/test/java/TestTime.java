import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @description
 * @author: LuoDeSong 694118297@qq.com
 * @create: 2019-08-28 14:24:21
 **/
public class TestTime {
    @Test
    public void dateToStamp() throws Exception {
        String str1 = "20170412030107";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String str2 = "20170412030007";
        Date date1 = simpleDateFormat.parse(str1);
        Date date2 = simpleDateFormat.parse(str2);
        long ts1 = date1.getTime();
        long ts2 = date2.getTime();
        long l = ts1 - ts2;

        String substring = "2018-09-13T02:15:16.054Z".substring(8, 10);
        System.out.println(substring);

    }
}
