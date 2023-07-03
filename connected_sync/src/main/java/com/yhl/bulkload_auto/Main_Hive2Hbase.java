package com.yhl.bulkload_auto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * 这里只适用于单列族的情况
 * java -cp tools.jar com.yhl.bulkload_auto.Main_Hive2Hbase \
 * hdfs://cdh1.xxxx.com:8020/user/hive/warehouse/xxx.db/test_yhl_hive_inner \
 * hdfs://cdh1.xxxx.com:8020/tmp \
 * test_yhl2 \
 * info \
 * v1,v2 \
 * username
 */
public class Main_Hive2Hbase {

    private static final Logger LOG = LoggerFactory.getLogger(Main_Hive2Hbase.class);

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", args[5]);

        String inputHivePath = args[0];
        String hfileTmpHdfsBasePath = args[1];
        String hbaseNameForCopy = args[2];
        String family = args[3];
        if(!args[4].contains(",")) {
            System.err.println("字段间以逗号分隔");
        }
        List<String> fieldNames = Arrays.asList(args[4].split(","));

        /**
         * 将hdfs的数据挂载到hbase，这里需要对数据进行处理
         * 1.从指定的hdfs路径的数据处理
         * 2.变为hfile
         * 3.将hfile数据挂载到hbase
         */

        /**
         * 临时存储生成的hfile的hdfs的地址
         */
        String[] tmps = inputHivePath.split("/");
        String hfileHdfsFilename = tmps[tmps.length-1];
        String outputHFilePath = Utils.hfileTmpHdfsPath(hfileTmpHdfsBasePath,hfileHdfsFilename);
        /**
         * 如果没有创建hbase表，提前创建hbase表
         */
        if(!HbaseOperate.isTableExist(hbaseNameForCopy)){
            //如果是hbase表到hbase表
            String[] familys = {family};
            HbaseOperate.createTable(hbaseNameForCopy, familys);
        }
        /**
         * 对hbase的列的处理，将列族和列名拼接起来
         */
        String hbaseFields = Utils.hbaseColumns(family, fieldNames);
        GenerateHFileLoadHbase.transToHFileAndLoad(inputHivePath,outputHFilePath,hbaseNameForCopy,hbaseFields);
        System.out.println("hive2hbase 拷贝完成,请查看hbase表 " + hbaseNameForCopy);
    }
}
