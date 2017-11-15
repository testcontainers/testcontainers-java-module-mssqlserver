package org.testcontainers.junit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.Test;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MSSQLServerWithDatabaseTest {

    @Test
    public void checkDatabaseExistsOnStartup() throws SQLException {
        // given: a database name
        final String databaseName = "foo";

        // given: a MSSQLServerContainer with specified database
        JdbcDatabaseContainer mssqlServerContainer = new MSSQLServerContainer()
                .withDatabaseName(databaseName);
        mssqlServerContainer.start();

        // when: checking if database exists
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(mssqlServerContainer.getJdbcUrl());
        hikariConfig.setUsername(mssqlServerContainer.getUsername());
        hikariConfig.setPassword(mssqlServerContainer.getPassword());

        HikariDataSource ds = new HikariDataSource(hikariConfig);
        PreparedStatement pst = ds.getConnection().prepareStatement("SELECT name FROM master.sys.databases WHERE name = ?");
        pst.setString(1, databaseName);
        ResultSet resultSet = pst.executeQuery();

        // then: result should contain the database name
        Assert.assertTrue(
                "ResultSet is empty and does not contain the required database name!",
                resultSet.next()
        );
        Assert.assertEquals(
                "Returned database name does not match the one which is expected!",
                resultSet.getString("name"),
                databaseName
        );
    }

    @Test
    public void checkCustomDatabaseWithCollation() throws SQLException {
        // given: a database name
        final String databaseName = "foo";
        final String collation = "Latin1_General_100_CS_AS_SC";

        // given: a MSSQLServerContainer with specified database
        JdbcDatabaseContainer mssqlServerContainer = new MSSQLServerContainer()
                .withDatabaseName(databaseName)
                .withCollation(collation);
        mssqlServerContainer.start();

        // when: checking if database exists
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(mssqlServerContainer.getJdbcUrl());
        hikariConfig.setUsername(mssqlServerContainer.getUsername());
        hikariConfig.setPassword(mssqlServerContainer.getPassword());

        HikariDataSource ds = new HikariDataSource(hikariConfig);
        PreparedStatement pst = ds.getConnection().prepareStatement("SELECT collation_name FROM sys.databases WHERE name = ?");
        pst.setString(1, databaseName);
        ResultSet resultSet = pst.executeQuery();

        // then: result should contain the database name
        Assert.assertTrue(
                "ResultSet is empty and the required database has not been created!",
                resultSet.next()
        );
        Assert.assertEquals(
                "Returned collation does not match the one which is expected!",
                resultSet.getString("collation_name"),
                collation
        );
    }
}
