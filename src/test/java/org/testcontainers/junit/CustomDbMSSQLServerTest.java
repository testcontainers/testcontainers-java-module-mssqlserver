package org.testcontainers.junit;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.rnorth.visibleassertions.VisibleAssertions.*;

public class CustomDbMSSQLServerTest {

    private static final String DATABASE_NAME = "custom";
    private static final String COLLATION = "Cyrillic_General_CI_AS";

    @Rule
    public MSSQLServerContainer mssqlServer = new MSSQLServerContainer()
            .withDatabaseName(DATABASE_NAME)
            .withCollation(COLLATION);


    @Test
    public void testAutoSetUpCustomDb() throws SQLException {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(mssqlServer.getJdbcUrl());
        hikariConfig.setUsername(mssqlServer.getUsername());
        hikariConfig.setPassword(mssqlServer.getPassword());

        try (HikariDataSource ds = new HikariDataSource(hikariConfig)) {

            Statement statement = ds.getConnection().createStatement();
            ResultSet rs = statement.executeQuery("SELECT DB_NAME() AS dbName");

            rs.next();
            assertEquals(
                    "Current database SELECT query succeeds",
                    DATABASE_NAME,
                    rs.getString(1)
            );

            PreparedStatement preparedStatement = ds.getConnection()
                    .prepareStatement("SELECT collation_name FROM sys.databases WHERE name = ?");

            preparedStatement.setString(1, DATABASE_NAME);
            rs = preparedStatement.executeQuery();
            rs.next();
            assertEquals("", COLLATION, rs.getString(1));
        }
    }
}
