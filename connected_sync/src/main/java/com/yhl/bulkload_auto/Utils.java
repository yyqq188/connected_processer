package com.yhl.bulkload_auto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {
    /**
     * 对hbase的表名进行处理 转为 hive的内部表名和外部表名
     * 如果有namespace,改为namespace_table ,否者就只有tablename
     * hive的内部表名
     * namespace_table_hive_inner or table_hive_inner
     * hive的外部表名
     * namespace_table_hive_outer or table_hive_outer
     */
    public static Map<String,String> tableNameTransform(String hbaseTableName) {
        String lowerHbaseTableName = hbaseTableName.toLowerCase();
        Map<String,String> hiveTableName = new HashMap<>();
        if(lowerHbaseTableName.contains(":")){
            String namespace = lowerHbaseTableName.split(":")[0];
            String tableName = lowerHbaseTableName.split(":")[1];
            hiveTableName.put("outer",namespace +"_"+ tableName+"_"+ "hive_outer");
            hiveTableName.put("inner",namespace +"_"+ tableName+"_"+ "hive_inner");
        }else{
            hiveTableName.put("outer",lowerHbaseTableName+"_"+ "hive_outer");
            hiveTableName.put("inner",lowerHbaseTableName+"_"+ "hive_inner");
        }
        return hiveTableName;
    }

    /**
     * 这里使用字符串拼接
     * 生产外部表的sql语句
     */
    public static String createOuterSql(List<String> fieldNames,String hiveTableName,String hbaseTableName) {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        StringBuilder sb3 = new StringBuilder();

        sb1.append("create external table if not exists ");
        sb1.append(hiveTableName);
        sb1.append(" (\n");
        sb1.append("rowkey string,\n");
        for(String fieldName:fieldNames){
            sb1.append(fieldName);
            sb1.append(" ");
            sb1.append("string,");
            sb1.append("\n");
        }
        String str1 = sb1.toString();
        String str2 = str1.substring(0,str1.length()-2);
        sb2.append(str2);
        sb2.append(")STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\n");
        sb2.append("WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \"\n");
        sb2.append(":key,\n");
        for(String fieldName:fieldNames){
            sb2.append("info:");
            sb2.append(fieldName);
            sb2.append(",");
            sb2.append("\n");
        }
        String str3 = sb2.toString();
        String str4 = str3.substring(0,str3.length()-2);
        sb3.append(str4);
        sb3.append("\")\n");
        sb3.append("TBLPROPERTIES (\"hbase.table.name\" = \"");
        sb3.append(hbaseTableName);
        sb3.append("\")");
        return sb3.toString();


    }

    /**
     * 生产内部表的sql语句
     */
    public static String createInnerSql(String hiveOuterTableName,String hiveInnerTableName){
        return "create table if not exists "+hiveInnerTableName+" as select * from " + hiveOuterTableName;
    }

    public static String innerHiveHdfsPath(String hiveBasePath,String urlHive,String tableName){
        String[] hiveDbNames = urlHive.split("/");
        String hiveDbName = hiveDbNames[hiveDbNames.length-1];
        return hiveBasePath + "/" +hiveDbName + ".db"+ "/" + tableName;
    }
    /**
     * 生成临时的存储hfile的hdfs的地址
     */
    public static String hfileTmpHdfsPath(String hfileTmpHdfsBasePath,String innerHiveName) {
        return hfileTmpHdfsBasePath +"/"+ innerHiveName +"_2";
    }

    /**
     *
     * String family = "info";
     * List<String> fieldNames = new ArrayList<>();
     * fieldNames.add("v1");
     * fieldNames.add("v2");
     * 结果是
     * info:v1,info:v2
     */
    public static String hbaseColumns(String family,List<String> fieldNames) {
        StringBuilder sb = new StringBuilder();
        for(String fieldName:fieldNames){
            sb.append(family +":"+fieldName + ",");
        }
        String s = sb.toString();
        return s.substring(0,s.length()-1);
    }

    public static void main(String[] args) throws Exception {
        String family = "info";
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("v1");
        fieldNames.add("v2");
        System.out.println(hbaseColumns(family,fieldNames));


    }
}
