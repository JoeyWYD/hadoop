package com.shujia;

import java.sql.*;

public class Query {
    public static void main(String[] args) throws SQLException {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://192.168.163.128:10000/shujia001","root","0613");

        Statement statement = con.createStatement();
        String sql = "select * from words";
        ResultSet res = statement.executeQuery(sql);
        while (res.next()){
            System.out.println(res.getObject(1));
        }
        statement.close();
        con.close();
    }
}
