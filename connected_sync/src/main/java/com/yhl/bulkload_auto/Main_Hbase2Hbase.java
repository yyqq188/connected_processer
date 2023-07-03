package com.yhl.bulkload_auto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

/**
 * 这里只适用于单列族的情况
 * java -cp tools.jar com.yhl.bulkload_auto.Main_Hbase2Hbase 10.1.100.11:10000/nci_hive_dfs_mob \
 * username \
 * password \
 * hdfs://cdh1.xxxx.com:8020/user/hive/warehouse \
 * hdfs://cdh1.xxxx.com:8020/tmp \
 * test_yhl \
 * test_yhl6 \
 * info \
 * v1,v2
 */
public class Main_Hbase2Hbase {
    private static final Logger LOG = LoggerFactory.getLogger(Main_Hbase2Hbase.class);
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", args[1]);
        String urlHive = args[0];
        String userHive = args[1];
        String pwdHive = args[2];
        String hiveBasePath = args[3];
        String hfileTmpHdfsBasePath = args[4];
        //todo 这部分要修改，为了好区分测试环境和开发环境
        //这是hbase的source表
        String tableNameHbase = args[5];
        //这是hbase的sink表
        String hbaseNameForCopy = args[6];
        String family = args[7];
        if(!args[8].contains(",")) {  //v1,v2
            System.err.println("字段间以逗号分隔");
        }
        List<String> fieldNames = Arrays.asList(args[8].split(","));


        /**
         * 先执行创建外部表以及创建内部表的动作
         * 将hbase的表名转为hive的内部表和外部表的表名
         */
        String hiveOuterTableName = Utils.tableNameTransform(tableNameHbase).get("outer");
        String hiveInnerTableName = Utils.tableNameTransform(tableNameHbase).get("inner");
        System.out.println("hiveOuterTableName = " + hiveOuterTableName);
        System.out.println("hiveInnerTableName = " + hiveInnerTableName);


        /**
         * 创建外部表的sql语句
         */
        String outerTableSql = Utils.createOuterSql(fieldNames, hiveOuterTableName, tableNameHbase);
        /**
         * 创建内部表的sql语句
         */
        String innerTableSql = Utils.createInnerSql(hiveOuterTableName, hiveInnerTableName);

        Connection hiveConn = GetHiveConn.getHiveConn(urlHive, userHive, pwdHive);
        try {
            //执行建立外部表的命令
            PreparedStatement preparedStatement = hiveConn.prepareStatement(outerTableSql);
            boolean execute = preparedStatement.execute();
            if (execute == false) {
                System.out.println("创建外部表处理成功");
                //sql 创建内部表
                PreparedStatement preparedStatement2 = hiveConn.prepareStatement(innerTableSql);
                boolean execute2 = preparedStatement2.execute();
                if (execute2 == false) {
                    System.out.println("创建内部表处理成功");
                } else {
                    System.out.println("创建内部表处理失败");
                }
            } else {
                System.out.println("创建外部表处理失败");
            }
        }finally {
            hiveConn.close();
        }

        /**
         * 将hdfs的数据挂载到hbase，这里需要对数据进行处理
         * 1.从指定的hdfs路径的数据处理
         * 2.变为hfile
         * 3.将hfile数据挂载到hbase
         */

        /**
         * 需要拼接内部表的hdfs路径
         */
        String inputHivePath = Utils.innerHiveHdfsPath(hiveBasePath, urlHive, hiveInnerTableName);
        /**
         * 临时存储生成的hfile的hdfs的地址
         */
        String outputHFilePath = Utils.hfileTmpHdfsPath(hfileTmpHdfsBasePath,hiveInnerTableName);
        /**
         * 如何没有创建hbase表，提前创建hbase表
         */
        if(!HbaseOperate.isTableExist(hbaseNameForCopy)){
            //如果是hbase表到hbase表
            HbaseOperate.createTable(tableNameHbase,hbaseNameForCopy, HbaseOperate.connection);
        }
        /**
         * 对hbase的列的处理，将列族和列名拼接起来
         */
        String hbaseFields = Utils.hbaseColumns(family, fieldNames);

        GenerateHFileLoadHbase.transToHFileAndLoad(inputHivePath,outputHFilePath,hbaseNameForCopy,hbaseFields);
        System.out.println("hbase2hbase 拷贝完成,请查看hbase表 " + hbaseNameForCopy);
    }
}
