import bp.utils.HBaseAPI;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
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

        Map<String, String> dataWithoutClose = HBaseAPI.getDataWithoutClose("2017-07-19_869718026481488", a);
        System.out.println(dataWithoutClose);

//
        HBaseAPI.close();
////        bp.utils.HBaseAPI.init();
//        List<String> list = HBaseAPI.listTables(true);
//        System.out.println(list);
////        bp.utils.HBaseAPI.close();
    }
}