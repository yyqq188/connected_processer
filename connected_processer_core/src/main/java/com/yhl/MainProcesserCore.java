package com.yhl;

import java.util.Map;

public class MainProcesserCore {
    public static void main(String[] args) {
        //todo sql语句都要为小写 join语句不要加表的别名
        String createDriverTableSqlStr = "create table xx ";
        String createConnectedTableSqlStr = "create table xx ";
        String joinSqlStr = "select t1.a from t1 join t2 on t1.a = t2.b where t2.a > 0 ";
        //连接hive 获得 一些相关信息
        /**
         * 先要看joinSqlStr
         * 找到selectField 和 on Field
         * 然后看createSqlStr
         * 找到驱动流表
         * 根据create语句，获得所有关于driver表中的字段在connect表中也有的字段
         * 分别去检查所有的count
         * 然后基于on的关联字段 进一步检查，如果on的字段组合成rowkey的覆盖度是否能达到全表
         *
         * select的字段和on的字段的count数，是否是一一对应的关系
         */


    }

    /**
     *
     * @param driverTable
     * @param connectedTable
     * @param driverTablePrimaries
     * @param connectedTablePrimaries
     * @param driverTableJoinFields
     * @param connectedTableJoinFields
     * @param connectedFilerMap
     *
     * 返回的是rowkey 过滤的maper
     */

    public static void processCore(String driverTable,
                                   String connectedTable,
                                   String[] driverTablePrimaries,
                                   String[] connectedTablePrimaries,
                                   String[] driverTableJoinFields,
                                   String[] connectedTableJoinFields,
                                   Map<String,String> connectedFilerMap) {


    }

}
