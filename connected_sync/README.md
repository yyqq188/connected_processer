# 这里是将hive或hbase的数据同步到hbase中的程序，在同步的过程中，也可以添加处理逻辑
## 同步的过程中，处理的逻辑在mapper包下

## hive同步hbase的命令解释
```shell
java -cp tools.jar com.yhl.bulkload_auto.Main_Hive2Hbase \
#这是hive内部表的hdfs地址，利用desc hivetable命令查找
hdfs://cdh1.xxxx.com:xxx/user/hive/warehouse/nci_hive_dfs_mob.db/test_yhl_hive_inner \
#这是hfile在hdfs的临时存放地址
hdfs://cdh1.xxxx.com:xxx/tmp \
#这是需要到出到的hbase表（若没有会自动创建）
test_yhl2 \
#这是列族(只支持单列族)
info \
#这是hbase的列名，需要到hive中查找
v1,v2
```

## hbase同步hbase的命令解释
```shell
java -cp tools.jar com.yhl.bulkload_auto.Main_Hbase2Hbase \
#hive的地址(这里是测试环境)
90.90.90.90:10000/nci_hive_dfs_mob \
#hive的用户
username \
#hive的密码
password \
#hive在hdfs的基础地址
hdfs://cdh1.xxx.com:8020/user/hive/warehouse \
#hfile在hdfs的临时地址
hdfs://cdh1.xxx.com:8020/tmp \
#source的hbase 被拷贝的表
test_yhl \
#sink的hbase 拷贝到的表
test_yhl3 \
#列族(只支持单列族)
info \
#列名(通过hive来查)
v1,v2
#用户名
username
```