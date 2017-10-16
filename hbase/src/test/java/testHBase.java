import bp.utils.HBaseAPI;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created with Project: hbase
 * User: Casey
 * Date: 2017/6/29　16:50
 * Description:
 */
public class testHBase extends TestCase {
    public void test1() throws IOException {

        HBaseAPI.init();

        Table a = HBaseAPI.connection.getTable(TableName.valueOf("rd_malice_equipment_history"));
//        List<Map<String, String>> dataByValueFilterWithoutClose = HBaseAPI.getDataBySingleColumnValueFilterWithoutClose(a, "portrait","appver","4.4.4");
//
//        int i = 0;
//        for (Map<String, String> aa : dataByValueFilterWithoutClose) {
//            System.out.println(aa);
//            i++;
//            if (i == 10)
//                break;
//        }
//
//        System.out.println(dataByValueFilterWithoutClose.size());
//        List<String> list = HBaseAPI.listTables(false);
//        System.out.println(list);
//

//        Map<String, String> dataWithoutClose = HBaseAPI.getDataWithoutClose("2017-07-19_869718026481488", a);
//        System.out.println(dataWithoutClose);

        List<String> list = new ArrayList<String>() {{
            add("loginusers90d");
            add("imei");
        }};

//        Map<String, String> dataWithoutClose = HBaseAPI.getDataWithoutClose(a, "2017-08-02_911443700185028", "cf", list);
        List<Map<String, String>> allDataWithoutClose = HBaseAPI.getAllDataWithoutClose(a, "cf", Arrays.asList("imei", "loginusers90d"));
        System.out.println(allDataWithoutClose.size());

//
        HBaseAPI.close();
////        bp.utils.HBaseAPI.init();
//        List<String> list = HBaseAPI.listTables(true);
//        System.out.println(list);
////        bp.utils.HBaseAPI.close();
    }

    public void testList() throws IOException {
        List<String> list = HBaseAPI.listTables(true);
        System.out.println(list);

    }
}
