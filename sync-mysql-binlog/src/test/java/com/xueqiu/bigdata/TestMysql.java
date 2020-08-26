package com.xueqiu.bigdata;

import org.apache.commons.dbutils.DbUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

public class TestMysql {
    Connection conn = null;
    Statement stm = null;

    @Before
    public void before(){
        try {
            String url = "jdbc:mysql://k8s-master01:3306";
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url,"maxwell","maxwell");
            stm = conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDescTable(){
        try {
            ResultSet resultSet = stm.executeQuery("desc animal.dog");
            while (resultSet.next()){
                String columnName = resultSet.getString("Field");
                Object columnType = resultSet.getString("Type");
                Object columnKey = resultSet.getString("key");
                System.out.println(columnName+", " + columnType+", "+ columnKey);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
