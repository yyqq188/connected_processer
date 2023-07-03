package com.yhl.utils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlParser {
    public static final String REG_CREATE="(?i)create\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*primary\\s*key\\((.+)\\)";
    public static void main(String[] args) {
        String createSql  = "create table  table1(orderId ,gdsId,orderTime) primary key(orderId);";
        parseCreateSqlStr(createSql);

        String joinSqlStr = "select t1.a ,t2.b ,t1.c,t2.d from t1 join t2 on t1.f1 = t2.f2 and t1.f3=t2.f4 where t2.f3 > 0 and t2.f4 > 0";
//        String joinSqlStr = "select t1.a ,t2.b ,t1.c,t2.d from t1 join t2 on t1.f1 = t2.f2 and t1.f3=t2.f4 where t2.f3 > 0";
//        String joinSqlStr = "select t1.a ,t2.b from t1 join t2 on t1.f1 = t2.f2";
//        String joinSqlStr = "select t1.a ,t2.b ,t1.c,t2.d from t1 join t2 on t1.f1 = t2.f2 and t1.f3=t2.f4";
        parseJoinSqlStr(joinSqlStr);

    }
    public static void parseCreateSqlStr(String createSqlStr) {
        Pattern pattern= Pattern.compile(REG_CREATE);
        Matcher matcher = pattern.matcher(createSqlStr);
        if(matcher.find()) {
            String group1 = matcher.group(1);
            String group2 = matcher.group(2);
            String group3 = matcher.group(3);
            System.out.println(group1);
            System.out.println(group2);
            System.out.println(group3);
        }

    }
    public static void parseJoinSqlStr(String joinSqlStr) {
        Map<String,Map<String,List<String>>> driverTableAllInfo = new HashMap<>();
        Map<String,Map<String,List<String>>> connectedTableAllInfo = new HashMap<>();
        Map<String , List<String>> driverTableInfo = new HashMap<>();
        Map<String , List<String>> connectedTableInfo = new HashMap<>();
        String driverTableName = null;
        String connectedTableName = null;

        //解析驱动表和关联表的表名
        String tableTwoStr = joinSqlStr.split("from")[1].split("on")[0];
        driverTableName = tableTwoStr.split("join")[0].trim();
        connectedTableName = tableTwoStr.split("join")[1].trim();
        driverTableAllInfo.put(driverTableName,null);
        connectedTableAllInfo.put(connectedTableName,null);
        //解析select的字段
        String selectInfoStr = joinSqlStr.split("select")[1].split("from")[0];
        List<String> driverselectmp = new ArrayList<>();
        List<String> connectedselectmp = new ArrayList<>();
        for(String e:selectInfoStr.split(",")){
            if(e.trim().contains(driverTableName)){
                driverselectmp.add(e.trim().split("\\.")[1].trim());
            }
            if(e.trim().contains(connectedTableName)){
                connectedselectmp.add(e.trim().split("\\.")[1].trim());
            }
        }
        driverTableInfo.put("selectField",driverselectmp);
        connectedTableInfo.put("selectField",connectedselectmp);


        List<String> driverontmp = new ArrayList<>();
        List<String> connectedontmp = new ArrayList<>();
        List<String> connectedwheretmp = new ArrayList<>();
        if(joinSqlStr.contains("where")){
            //todo 只有connected表才用到
            String whereInfoStr = joinSqlStr.split("where")[1].trim();
            String whereInfoStrReplace = whereInfoStr.replace(".", "").replace(connectedTableName, "");

            if(whereInfoStrReplace.contains("and")){
                for(String e:whereInfoStrReplace.split("and")){
                    connectedwheretmp.add(e.trim());
                }

            }else{
                connectedwheretmp.add(whereInfoStrReplace.trim());
            }
            connectedTableInfo.put("whereField",connectedwheretmp);

            String onInfoStr = joinSqlStr.split("on")[1].trim().split("where")[0].trim();
            if(onInfoStr.contains("and")) {
                for(String ee:onInfoStr.split("and")){
                    for(String e:ee.trim().split("=")){
                        if(e.trim().contains(driverTableName)){
                            driverontmp.add(e.trim().split("\\.")[1].trim());
                        }
                        if(e.trim().contains(connectedTableName)){
                            connectedontmp.add(e.trim().split("\\.")[1].trim());
                        }
                    }
                }
            }else{
                for(String e:onInfoStr.trim().split("=")){
                    if(e.trim().contains(driverTableName)){
                        driverontmp.add(e.trim().split("\\.")[1].trim());
                    }
                    if(e.trim().contains(connectedTableName)){
                        connectedontmp.add(e.trim().split("\\.")[1].trim());
                    }
                }
            }
        }else{
            String onInfoStr = joinSqlStr.split("on")[1].trim();
            if(onInfoStr.contains("and")) {
                for(String ee:onInfoStr.split("and")){
                    for(String e:ee.trim().split("=")){
                        if(e.trim().contains(driverTableName)){
                            driverontmp.add(e.trim().split("\\.")[1].trim());
                        }
                        if(e.trim().contains(connectedTableName)){
                            connectedontmp.add(e.trim().split("\\.")[1].trim());
                        }
                    }
                }
            }else{
                for(String e:onInfoStr.trim().split("=")){
                    if(e.trim().contains(driverTableName)){
                        driverontmp.add(e.trim().split("\\.")[1].trim());
                    }
                    if(e.trim().contains(connectedTableName)){
                        connectedontmp.add(e.trim().split("\\.")[1].trim());
                    }
                }
            }
        }
        driverTableInfo.put("onField",driverontmp);
        connectedTableInfo.put("onField",connectedontmp);
        driverTableAllInfo.put("driverTableInfo",driverTableInfo);
        connectedTableAllInfo.put("connectedTableInfo",connectedTableInfo);


        System.out.println(driverTableAllInfo);
        System.out.println(connectedTableAllInfo);
    }
}