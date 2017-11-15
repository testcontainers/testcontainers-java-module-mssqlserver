package org.testcontainers.junit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Test;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.rnorth.visibleassertions.VisibleAssertions.assertEquals;

/**
 * @author Stefan Hufschmidt
 */
public class SimpleMSSQLServerTest {

    @Test
    public void testSimple() throws SQLException {
        MSSQLServerContainer mssqlServer = new MSSQLServerContainer();
        mssqlServer.start();
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(mssqlServer.getJdbcUrl());
        hikariConfig.setUsername(mssqlServer.getUsername());
        hikariConfig.setPassword(mssqlServer.getPassword());

        HikariDataSource ds = new HikariDataSource(hikariConfig);
        PreparedStatement statement = ds.getConnection().prepareStatement("SELECT 1");
        ResultSet resultSet = statement.executeQuery();

        resultSet.next();
        int resultSetInt = resultSet.getInt(1);
        assertEquals("A basic SELECT query succeeds", 1, resultSetInt);
    }

    @Test
    public void testSetupDatabase() throws SQLException {
        MSSQLServerContainer mssqlServer = new MSSQLServerContainer();
        mssqlServer.start();
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(mssqlServer.getJdbcUrl());
        hikariConfig.setUsername(mssqlServer.getUsername());
        hikariConfig.setPassword(mssqlServer.getPassword());

        HikariDataSource ds = new HikariDataSource(hikariConfig);
        Statement statement = ds.getConnection().createStatement();
        statement.executeUpdate("CREATE DATABASE [test];");
        statement = ds.getConnection().createStatement();
        statement.executeUpdate("CREATE TABLE [test].[dbo].[Foo](ID INT PRIMARY KEY);");
        statement = ds.getConnection().createStatement();
        statement.executeUpdate("INSERT INTO [test].[dbo].[Foo] (ID) VALUES (3);");
        statement = ds.getConnection().createStatement();
        statement.execute("SELECT * FROM [test].[dbo].[Foo];");
        ResultSet resultSet = statement.getResultSet();

        resultSet.next();
        int resultSetInt = resultSet.getInt("ID");
        assertEquals("A basic SELECT query succeeds", 3, resultSetInt);
    }
}
