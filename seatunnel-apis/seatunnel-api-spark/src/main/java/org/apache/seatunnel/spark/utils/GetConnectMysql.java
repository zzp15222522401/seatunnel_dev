package org.apache.seatunnel.spark.utils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class GetConnectMysql {

    public static void saveToMysql(String sql, String env) throws SQLException, IOException, ClassNotFoundException {

        String propFileName = String.format("jdbc_mysql_%s.properties", env);
        Properties properties = loadProperties(propFileName);
        String username = properties.getProperty("jdbc.username");
        String password = properties.getProperty("jdbc.password");
        String dbName = properties.getProperty("jdbc.dbName");
        String host = properties.getProperty("jdbc.host");
        String port = properties.getProperty("jdbc.port");
        Class.forName(properties.getProperty("jdbc.driverClass"));
        String url = "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?characterEncoding=utf-8&allowMultiQueries=true&useSSL=false";
        Connection conn = null;
        PreparedStatement preparedStatement = null;
        try {
            conn = DriverManager.getConnection(url, username, password);
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            assert preparedStatement != null;
            preparedStatement.close();
            conn.close();
        }
    }

    public static Properties loadProperties(String fileName) throws IOException {

        Properties properties = new Properties();
        InputStream propFile = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        properties.load(propFile);
        return properties;
    }
}
