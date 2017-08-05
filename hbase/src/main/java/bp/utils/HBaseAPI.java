package bp.utils;
/**
 * Created with Project: hbase
 * User: Casey
 * Date: 2017/6/29　16:50
 * Description:
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class HBaseAPI {
//    private static Logger logger = Logger.getLogger(bp.utils.HBaseAPI.class);

    public static Configuration conf;
    public static Connection connection;
    public static Admin admin;

    // 初始化
    public static void init() {
        if (conf == null) {
            conf = HBaseConfiguration.create();
            try {
                connection = ConnectionFactory.createConnection(conf);
                admin = connection.getAdmin();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //关闭连接
    public static void close() {
        try {
            if (null != admin)
                admin.close();
            if (null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //查看已有表
    public static List<String> listTables(boolean autoClose) throws IOException {
        if (autoClose) init();
        System.out.println("get hbase tables list. . . ");
        List<String> tableList = new ArrayList<String>();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            tableList.add(hTableDescriptor.getNameAsString());
        }
        if (autoClose) close();
        return tableList;
    }

    // 判断某表是否存在
    public static boolean isExist(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        boolean flag = admin.tableExists(tn);
        close();
        return flag;
    }

    public static boolean isExistWithoutClose(String tableName) throws IOException {
//        List<String> tableList = listTables(false);
//        for(String table:tableList){
//            if(table.equals(tableName))
//                return true;
//        }
//        return false;
//
//
        TableName tn = TableName.valueOf(tableName);
        boolean flag = admin.tableExists(tn);
        return flag;
    }

    // 建立数据库表
    public static void createTable(String tableName, List<String> cols, boolean autoClose) throws IOException {
        if (autoClose) init();

        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            System.out.println("talbe is exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tn);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        if (autoClose) close();
    }


    // 删除数据库表
    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }

    public static void deleteTableWithoutClose(String tableName) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
    }


    //插入一行数据
    public static void putRow(String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);

        //批量插入
       /* List<Put> putList = new ArrayList<Put>();
        puts.add(put);
        table.put(putList);*/
        table.close();
        close();
    }


    // 根据rowKey,插入多个列名的数据 Map 的key是col, vals是要插入的值
    public static void putRow(String tableName, String rowKey, String colFamily, Map<String, String> vals) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, String> val : vals.entrySet()) {
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(val.getKey()), Bytes.toBytes(val.getValue()));
        }
        table.put(put);
        table.close();
        close();
    }

    // 根据rowkey插入数据, 自定义关闭链接
    public static void putRowWithoutClose(Table table, String rowKey, String colFamily, Map<String, Object> vals) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, Object> val : vals.entrySet()) {
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(val.getKey()), Bytes.toBytes(val.getValue().toString()));
        }
        table.put(put);
    }


    // 批量插入数据
    public static void putRows(String tableName, List<Put> puts) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(puts);
        table.close();
        close();
    }


    // 批量插入数据, 自定义关闭链接
    public static void putRowsWithoutClose(Table table, List<Put> puts) throws IOException {
        table.put(puts);
    }


    //删除数据
    public static void deleRow(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        //删除指定列族
        //delete.addFamily(Bytes.toBytes(colFamily));
        //删除指定列
        //delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        //批量删除
       /* List<Delete> deleteList = new ArrayList<Delete>();
        deleteList.add(delete);
        table.delete(deleteList);*/
        table.close();
        close();
    }

    //根据rowkey,列族, 列名查找数据
    public static Map<String, String> getData(String tableName, String rowKey, String colFamily, List<String> colList) throws IOException {
//        List<Map<String, String>> records;
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //获取指定列族数据
        if (colFamily.length() != 0)
            get.addFamily(Bytes.toBytes(colFamily));
        //获取指定列数据
        for (String col : colList) {
            if (col.length() != 0)
                get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }
        Result result = table.get(get);
//        records = showCell(result);
        table.close();
        close();
        return showCell(result);
    }

    public static Map<String, String> getDataWithoutClose(Table table, String rowKey, String colFamily, List<String> colList) throws IOException {
        if (!isExistWithoutClose(table.getName().getNameAsString())) {
            return new HashMap<String, String>();
        }
        Get get = new Get(Bytes.toBytes(rowKey));
        //获取指定列族数据
        if (colFamily.length() != 0)
            get.addFamily(Bytes.toBytes(colFamily));
        //获取指定列数据
        for (String col : colList) {
            if (col.length() != 0)
                get.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }
        Result result = table.get(get);
        return showCell(result);
    }

    //    //  根据rowKey, 列族 获取数据
    public static Map<String, String> getData(String tableName, String rowKey, String colFamily) throws IOException {
        return getData(tableName, rowKey, colFamily, Arrays.asList(""));
    }

    //  根据rowKey 获取数据
    public static Map<String, String> getData(String tableName, String rowKey) throws IOException {
        return getData(tableName, rowKey, "", Arrays.asList(""));
    }


    // 可手动关闭连接, 提高循环读取数据时候的速度
    public static Map<String, String> getDataWithoutClose(String rowKey, Table table) throws IOException {

        if (!isExistWithoutClose(table.getName().getNameAsString())) {
            return new HashMap<String, String>();
        }

        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        return showCell(result);
    }


    // 指定范围查找数据
    public static List<Map<String, String>> getRangeData(String tableName, String startRow, String stopRow) throws IOException {
        List<Map<String, String>> records = new ArrayList<Map<String, String>>();
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            records.add(showCell(result));
        }
        table.close();
        close();
        return records;
    }

    public static List<Map<String, String>> getRangeDataWithoutClose(Table table, String startRow, String stopRow) throws IOException {
        List<Map<String, String>> records = new ArrayList<Map<String, String>>();
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            records.add(showCell(result));
        }
        return records;
    }

    // 添加基于行健的过滤器, 行健前缀
    public static List<Map<String, String>> getDataByRowkeyFilterWithoutClose(Table table, String rowKeyPre) throws IOException {
        List<Map<String, String>> records = new ArrayList<Map<String, String>>();
        Scan scan = new Scan();

        Filter filter = new PrefixFilter(Bytes.toBytes(rowKeyPre));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            records.add(showCell(result));
        }
        return records;
    }


    // 单列按值过滤器
    public static List<Map<String, String>> getDataBySingleColumnValueFilterWithoutClose(Table table, String family, String column, String key) throws IOException {
        List<Map<String, String>> records = new ArrayList<Map<String, String>>();
        Scan scan = new Scan();

//        Filter filter = new PrefixFilter(Bytes.toBytes(rowKeyPre));
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(key));
        filter.setFilterIfMissing(true); //所有不包含参考列的行都可以被过滤掉，不写的话不包含参考列的数据包含在结果中
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            records.add(showCell(result));
        }
        return records;
    }


    // 获取表中所有数据
    public static List<Map<String, String>> getAllData(String tableName) throws IOException {

        if (!isExistWithoutClose(tableName)) {
            System.out.println(tableName + "is not exist");
            return new ArrayList<>();
        }

        List<Map<String, String>> records = new ArrayList<Map<String, String>>();
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            records.add(showCell(result));
        }
        table.close();
        close();
        return records;
    }

    public static List<Map<String, String>> getAllDataWithoutClose(Table table) throws IOException {
        List<Map<String, String>> records = new ArrayList<Map<String, String>>();
//        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            records.add(showCell(result));
        }
        return records;
    }

    public static List<Map<String, String>> getAllDataWithoutClose(Table table,String colFamily,List<String> colList) throws IOException {
        List<Map<String, String>> records = new ArrayList<Map<String, String>>();
        Scan scan = new Scan();
        for(String col: colList) {
            scan.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        }

        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            records.add(showCell(result));
        }
        return records;
    }

    //格式化输出, 将rowkey也添加进了结果中.
    private static Map<String, String> showCell(Result result) {
        Cell[] cells = result.rawCells();
//        System.out.println(cells.length);
//        List<Map<String, String>> records = new ArrayList<Map<String, String>>();
        Map<String, String> record = new HashMap<String, String>();
        for (Cell cell : cells) {
//            Map<String, String> record = new HashMap<String, String>();
            record.put("rowkey", new String(CellUtil.cloneRow(cell))); // 将行健添加到结果中
            record.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
//            record.put("colFamily", new String(CellUtil.cloneFamily(cell)));
//            record.put("col", new String(CellUtil.cloneQualifier(cell)));
//            records.add(record);
        }
        return record;
    }

    /**
     * 将映射 Map 转为Hbase的Put形式
     *
     * @param rowKey    the row key
     * @param colFamily 列族
     * @param valsMap   the vals map
     * @return the put
     */
    public static Put map2Put(String rowKey, String colFamily, HashMap<String, Object> valsMap) {
        Put put = new Put(Bytes.toBytes(rowKey));
        // 将所有数据添加到put中并返回
        for (Map.Entry<String, Object> val : valsMap.entrySet()) {
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(val.getKey()), Bytes.toBytes(val.getValue().toString()));
        }
        return put;
    }

    // 根据key删除指定行的指定列们
    public static void deleteColumns(Table table, String rowKey, String family, String... colmuns) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        for (String str : colmuns)
            delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(str));
        table.delete(delete);
    }
}

