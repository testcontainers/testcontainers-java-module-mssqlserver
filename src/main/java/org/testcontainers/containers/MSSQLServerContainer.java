package org.testcontainers.containers;

import org.rnorth.ducttape.unreliables.Unreliables;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author Stefan Hufschmidt
 */
public class MSSQLServerContainer<SELF extends MSSQLServerContainer<SELF>> extends JdbcDatabaseContainer<SELF> {
    static final String NAME = "mssqlserver";
    static final String IMAGE = "microsoft/mssql-server-linux";
    public static final Integer MS_SQL_SERVER_PORT = 1433;
    private String username = "SA";
    private String password = "A_Str0ng_Required_Password";

    public MSSQLServerContainer() {
        this(IMAGE + ":latest");
    }

    public MSSQLServerContainer(final String dockerImageName) {
        super(dockerImageName);
    }

    @Override
    protected Integer getLivenessCheckPort() {
        return getMappedPort(MS_SQL_SERVER_PORT);
    }

    @Override
    protected void configure() {

        addExposedPort(MS_SQL_SERVER_PORT);
        addEnv("ACCEPT_EULA", "Y");
        addEnv("SA_PASSWORD", password);
    }

    @Override
    public String getDriverClassName() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:sqlserver://" + getContainerIpAddress() + ":" + getMappedPort(MS_SQL_SERVER_PORT);
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getTestQueryString() {
        return "SELECT 1";
    }

    // TODO: Replace with permanent solution to https://github.com/testcontainers/testcontainers-java/issues/568
    @Override
    public Connection createConnection(String queryString) throws SQLException {
        final Properties info = new Properties();
        info.put("user", this.getUsername());
        info.put("password", this.getPassword());
        final String url = this.getJdbcUrl() + queryString;

        final Driver jdbcDriverInstance = getJdbcDriverInstance();

        try {
            return Unreliables.retryUntilSuccess(120, TimeUnit.SECONDS, () -> jdbcDriverInstance.connect(url, info));
        } catch (Exception e) {
            throw new SQLException("Could not create new connection", e);
        }
    }
}
