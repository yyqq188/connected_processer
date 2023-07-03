package com.yhl.bulkload_auto.mapper;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    /**
     * 这个方法是需要定制去写的
     */

    public static String changeRowkey(Text value) {
        //切分导入的数据
        String values=value.toString();
        String[] lines=values.split("\u0001");
        //String[] Lines=Values.split("\u0001");
        ////BRANCHTYPE RISKCODE YEAR PAYINTV CURYEAR MANAGECOM F01 F03 F07 STARTDATE ENDDATE INDEXTYPE
        //String originalRowkey = Lines[1] +"_"+Lines[0]+"_"+Lines[6]+"_"+Lines[7]+"_"+Lines[8]+"_"+Lines[24]
        //        +"_"+Lines[9]+"_"+Lines[11]+"_"+Lines[25]+"_"+Lines[20]+"_"+Lines[21]+"_"+Lines[19];
        //
        //
        //
        //if(originalRowkey.contains("_")) {
        //    String[] rowkeys = originalRowkey.split("_");
        //    String primaryKey = rowkeys[1];
        //    String lastTwo = primaryKey.substring(primaryKey.length() - 2);
        //    return primaryKey+"_"+lastTwo;
        //}else{
        //    return "";
        //}
        return lines[0];
    }




    @Override
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {
        String[] hbaseFieldList = null;
        Configuration configuration = context.getConfiguration();
        String hbaseFields = configuration.get("fieldStr");
        if(hbaseFields.contains(",")){
            hbaseFieldList = hbaseFields.split(",");
        }else{
            System.err.println("hbase字段值没有以逗号分隔");
        }



        String rowkey=changeRowkey(value);
        if (Strings.isNullOrEmpty(rowkey)) return;

        ImmutableBytesWritable putRowkey=new ImmutableBytesWritable(rowkey.getBytes());
        Put put=new Put(rowkey.getBytes());

        //切分导入的数据
        String values=value.toString();
        String[] lines=values.split("\u0001");

        for (int i = 1; i < lines.length; i++) {
            if(!"\\N".equals(lines[i])&&!"NULL".equals(lines[i])){
                String family = hbaseFieldList[i-1].split(":")[0];
                String column = hbaseFieldList[i-1].split(":")[1];
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(lines[i]));
            }
        }
        //todo 最后这部分需要改？？？？
        ////if(!"\\N".equals(Lines[28])&&!"NULL".equals(Lines[28])){put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("PAYMONTH"),Bytes.toBytes(Lines[28]));}
        //if(Lines.length > 28 &&  !"\\N".equals(Lines[28])&&!"NULL".equals(Lines[28])){put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("PAYMONTH"),Bytes.toBytes(Lines[28]));}

        context.write(putRowkey,put);
    }

    public static String transformRowKey(String rowKey){
        int i = Math.abs(rowKey.hashCode()) % 100;
        String result = "";
        if(10 > i){
            result = "" + i + "_" + rowKey;
        }else {
            result = i + "_" + rowKey;
        }
        return result;
    }
}
