package com.xueqiu.bigdata.common.jdbcManager;

import org.apache.commons.dbutils.DbUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class AbstractJDBCManager {
    Connection conn = null;
    Statement stm = null;

    abstract  public void createConnection(String url, String username, String password) throws Exception;

    public boolean execute(String sql) throws SQLException {
        return  stm.execute(sql);
    }

    public ResultSet execueQuery(String sql) throws SQLException {
        return  stm.executeQuery(sql);
    }

    public void closeConnection() throws SQLException {
        DbUtils.close(stm);
        DbUtils.close(conn);
    }
}
