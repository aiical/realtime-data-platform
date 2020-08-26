package com.xueqiu.bigdata.common.jdbcManager;

import java.sql.DriverManager;

public class ImpalaJdbcManager extends AbstractJDBCManager {
    @Override
    public void createConnection(String url,String username, String password) throws Exception{
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        this.conn = DriverManager.getConnection(url, username, password);
        this.stm = conn.createStatement();
    }
}
