package com.yhl.bulkload_auto;

import com.yhl.bulkload_auto.mapper.MyMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GenerateHFileLoadHbase {
    public static void transToHFileAndLoad(String inputHivePath,String outputHFilePath,
                                           String outputHbaseName,String hbaseFields) throws Exception{
        Connection conn = HbaseOperate.connection;
        Configuration conf = conn.getConfiguration();
        //传递参数
        conf.set("fieldStr",hbaseFields);
        Table table = conn.getTable(TableName.valueOf(outputHbaseName));
        Admin admin = conn.getAdmin();

        final Path OutputPath = new Path(outputHFilePath);

        //设置相关类名
        Job job = Job.getInstance(conf, outputHbaseName);

        job.setJarByClass(GenerateHFileLoadHbase.class);
        //todo 这里需要修改导入的类
        job.setMapperClass(MyMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        //设置文件的输入路径和输出路径
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        FileInputFormat.setInputPaths(job, inputHivePath);
        FileOutputFormat.setOutputPath(job, OutputPath);

        //配置MapReduce作业，以执行增量加载到给定表中。
        HFileOutputFormat2.configureIncrementalLoad
                (job, table, conn.getRegionLocator(TableName.valueOf(outputHbaseName)));

        //MapReduce作业完成，告知RegionServers在哪里找到这些文件,将文件加载到HBase中
        if (job.waitForCompletion(true)) {
            LoadIncrementalHFiles Loader = new LoadIncrementalHFiles(conf);
            Loader.doBulkLoad(OutputPath, admin, table, conn.getRegionLocator(TableName.valueOf(outputHbaseName)));
        }
        deleteFile(OutputPath);
    }

    private static void deleteFile(Path path) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = path.getFileSystem(conf);

            boolean isok = fs.deleteOnExit(path);
            if (isok) {
                System.out.println("delete file " + path.getName() + " success!");
            } else {
                System.out.println("delete file " + path.getName() + " failure!!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
