package com.javaedge.canal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ClickHouseJDBCApp {
    public static void main(String[] args) throws Exception {

        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://10.51.51.48:8123/default";
        // 用户名和密码
        String user = "default";
        String password = "icv@2022";
        Connection connection = DriverManager.getConnection(url, user, password);
        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery("select * from my_first_table");
        while (rs.next()) {
            int id = rs.getInt("user_id");
            String name = rs.getString("message");
            System.out.println(id + "==>" + name);
        }

        rs.close();
        stmt.close();
        connection.close();
    }
}
