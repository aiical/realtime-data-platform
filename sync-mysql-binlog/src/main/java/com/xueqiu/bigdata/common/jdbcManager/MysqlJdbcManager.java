package com.xueqiu.bigdata.common.jdbcManager;

import java.sql.DriverManager;

public class MysqlJdbcManager extends AbstractJDBCManager {
    @Override
    public void createConnection(String url,String username, String password) throws Exception{
        Class.forName("com.mysql.jdbc.Driver");
        this.conn = DriverManager.getConnection(url, username, password);
        this.stm = conn.createStatement();
    }
}
