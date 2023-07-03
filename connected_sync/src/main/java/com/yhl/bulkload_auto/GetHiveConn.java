package com.yhl.bulkload_auto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;


public class GetHiveConn {
    private static final Logger LOG = LoggerFactory.getLogger(GetHiveConn.class);

    public static Connection getHiveConn(String url, String userName, String passWord) throws Exception {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        Connection conn = null;
        Class.forName(driverName);
        conn = DriverManager.getConnection("jdbc:hive2://" + url, userName, passWord);
        return conn;
    }
}
