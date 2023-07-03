package com.yhl.bulkload_auto;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * hbase
 * DDL 创建表
 * DML 增刪改查，以及全表扫描
 */
public class HbaseOperate {

    public static Connection connection = null;
    static{
        try {
            Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.addResource(new Path("hbase-site.xml"));
            hbaseConf.addResource(new Path("core-site.xml"));
            //hbaseConf.set("hbase.client.ipc.pool.size","10");
            connection = ConnectionFactory.createConnection(hbaseConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 获得变连接
     */
    public static Table getTable(String tableName) throws IOException{
        return connection.getTable(TableName.valueOf(tableName));
    }
    /**
     * 判断表是否存在
     */
    public static boolean isTableExist(String tableName) throws IOException {
        return connection.getAdmin().tableExists(TableName.valueOf(tableName));
    }

    /**
     * 创建表
     * 基于源表的结构建立sink表
     */
    public static void createTable(String tableNameSource,String tableNameSink,Connection connection) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableNameSource));
        ColumnFamilyDescriptor[] columnFamilies = table.getDescriptor().getColumnFamilies();
        List<String> familyNames = new ArrayList<>();
        for(ColumnFamilyDescriptor fs : columnFamilies){
            //System.out.println(fs.getName());
            familyNames.add(fs.getNameAsString());
        }
        //list 转 array
        String[] arr = new String[familyNames.size()];
        createTable(tableNameSink,familyNames.toArray(arr));
    }
    public static void createTable(String tableName,String... cfs) throws IOException {
        if(cfs.length <= 0){
            System.out.println("请设置列族信息!");
            return;
        }
        if(isTableExist(tableName)){
            System.out.println(tableName + "表已存在！");
            return;
        }
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String cf : cfs) {
            ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
            tableDescriptorBuilder.setColumnFamily(family);
        }
        connection.getAdmin().createTable(tableDescriptorBuilder.build());
    }
    /**
     * 删除表
     */
    public static void delData(String tableName) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Admin admin = connection.getAdmin();
        admin. disableTableAsync(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        System.out.println("删除成功");
    }

    /**
     * 插入单条数据
     */
    public static void insertData(String tableName,
                                  String rowKey,
                                  String colFamily,
                                  String col,
                                  String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(),col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
    }
    /**
     * 随机查询单条数据的列
     */
    public static Result getDataWithCol(String tableName,String rowKey,String colFamily, String col)throws  IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(),col.getBytes());
        Result result = table.get(get);
        table.close();
        return result;
    }

    /**
     *
     */
    public static Result getData(String tableName,String rowKey)throws  IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        table.close();
        return result;
    }
    /**
     * 关闭资源
     */
    public static void closeHbase() throws IOException {
        connection.getAdmin().close();

        if(connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void copyHbaseTable(String sourceTable,String sinkTable) throws IOException {
        Table source = connection.getTable(TableName.valueOf(sourceTable));
        Table sink = connection.getTable(TableName.valueOf(sinkTable));
        Scan scan1 = new Scan();
        ResultScanner scanner1 = source.getScanner(scan1);
        int count = 0;
        List<Put> puts = new ArrayList<>();
        for (Result res : scanner1) {
            String rowkey = Bytes.toString(res.getRow());
            System.out.println(rowkey);
            //修改
            //String s = rowkey.split("_",2)[1];
            //String substring = s.substring(s.length() - 2);
            //String aa = substring+"_"+s;

            String aa = rowkey;

            Put put = new Put(Bytes.toBytes(aa));
            List<Cell> cells = res.listCells();
            for(Cell cell:cells){
                //byte[] family = Bytes.toBytes("info");
                byte[] family = Bytes.toBytes("cf1");
                byte[] colume = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                put.addColumn(family,colume,value);
            }
            count += 1;
            System.out.println(count);

            puts.add(put);
            if(puts.size() == 10000){
                sink.put(puts);
                puts.clear();
                System.out.println("已插入10000条");
            }
        }
        sink.put(puts);

        closeHbase();
    }

    /**
     *hbase limit操作 例如查询10条数据
     */
    public static Map<String, Map<String,String>> limit(String sourceTable,int num) throws IOException {
        Table source = connection.getTable(TableName.valueOf(sourceTable));
        Map<String, Map<String,String>> rs = new HashMap<>();

        Scan scan = new Scan().setLimit(num);
        ResultScanner scanner = source.getScanner(scan);
        for(Result r:scanner){
            Map<String,String> columnWithValue = new HashMap<>();
            for(Cell cell:r.listCells()){
                columnWithValue.put(Bytes.toString(CellUtil.cloneFamily(cell)) +";"+
                        Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
            rs.put(Bytes.toString(r.getRow()),columnWithValue);

        }
        return rs;

    }

    /**
     * hbase limit操作并插入到另外一张表中 例如查询TableA 10条数据后插入到TableB
     */
    public static void limit(String sourceTable,String sinkTable,int num) throws IOException {
        Table source = connection.getTable(TableName.valueOf(sourceTable));
        Table sink = connection.getTable(TableName.valueOf(sinkTable));
        List<Put> puts = new ArrayList<>();

        Scan scan = new Scan().setLimit(num);
        ResultScanner scanner = source.getScanner(scan);
        for(Result r:scanner){
            Put put = new Put(r.getRow());
            for(Cell cell:r.listCells()){
                put.addColumn(CellUtil.cloneFamily(cell),CellUtil.cloneQualifier(cell),CellUtil.cloneValue(cell));
            }
            puts.add(put);
            if(puts.size() == 5000){
                sink.put(puts);
                puts.clear();
                System.out.println("已插入5000条");
            }

        }
        sink.put(puts);
        System.out.println("put num is " +num);

    }
    /**
     * hbase copy hbaseA到hbaseB，同时更改hbase表的rowkey
     * 这里是针对老核心的表进行的修改
     */

    public static void copyOldCoreWithChangeRowkey(String sourceTable,String sinkTable) throws Exception {
        Table source = connection.getTable(TableName.valueOf(sourceTable));
        Table sink = connection.getTable(TableName.valueOf(sinkTable));
        List<Put> puts = new ArrayList<>();

        Scan scan = new Scan();
        ResultScanner scanner = source.getScanner(scan);
        int count = 0;
        for(Result r:scanner){
            //这里是修改rowkey
            String newHashRowkey = changeOldCoreRowkey(sourceTable,Bytes.toString(r.getRow()));
            Put put = new Put(Bytes.toBytes(newHashRowkey));
            count += 1;
            System.out.println(count);
            for(Cell cell:r.listCells()){
                //put.addColumn(CellUtil.cloneFamily(cell),CellUtil.cloneQualifier(cell),CellUtil.cloneValue(cell));
                put.addColumn(Bytes.toBytes("cf1"),CellUtil.cloneQualifier(cell),CellUtil.cloneValue(cell));
            }
            puts.add(put);
            if(puts.size() == 5000){
                sink.put(puts);
                puts.clear();
                System.out.println("已插入5000条");
            }

        }
        sink.put(puts);
        System.out.println("修改rowkey并插入完毕");

    }


    /**
     * 对rowkey进行哈希处理，为了匹配中科软的rowkey逻辑
     * 将老逻辑进行拆分
     *
     * 老核心表的处理
     * LJAGETCLAIM
     * LJAGETENDORSE
     * LJAPAYPERSON
     */
    public static String changeOldCoreRowkey(String tableName,String rowkey) throws Exception{
        String lowCaseTableName = tableName.toLowerCase();
        if(lowCaseTableName.contains("ljagetclaim")){
            String[] rowkeystrs = rowkey.split("_");
            String actugetno = rowkeystrs[1];
            String feeoperationtype = rowkeystrs[2];
            String subfeeoperationtype = rowkeystrs[3];
            String feefinatype = rowkeystrs[4];
            String dutycode = rowkeystrs[5];
            String getdutykind = rowkeystrs[6];
            String getdutycode = rowkeystrs[7];
            String polno = rowkeystrs[8];

            String newRowkeyStr = actugetno+"_"+polno+"_"+getdutycode+"_"+getdutykind+"_"+
                    dutycode+"_"+feefinatype+"_"+subfeeoperationtype+"_"+feeoperationtype;
            System.out.println(newRowkeyStr);
            return specialRowKeyTransform(newRowkeyStr);

        }
        if(lowCaseTableName.contains("ljagetendorse")){
            String[] rowkeystrs = rowkey.split("_",2);
            String rowkeystr = rowkeystrs[1];
            System.out.println(rowkeystr);
            return specialRowKeyTransform(rowkeystr);

        }
        if(lowCaseTableName.contains("ljapayperson")){
            String[] rowkeystrs = rowkey.split("_",2);
            String rowkeystr = rowkeystrs[1];
            System.out.println(rowkeystr);
            return specialRowKeyTransform(rowkeystr);
        }

        return "";

    }

    public static String specialRowKeyTransform(String rowkey){
        //主键hash值模100取绝对值
        int a = Math.abs(rowkey.hashCode()) % 100;
        String newRowkey = "";
        if(a < 10){
            newRowkey = "" + a + "_" + rowkey;
        }else{
            newRowkey = a + "_" + rowkey;
        }
        //返回值 为 a_rowKey
        return newRowkey;
    }


}
