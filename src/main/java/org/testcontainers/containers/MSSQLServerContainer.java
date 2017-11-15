package org.testcontainers.containers;

import java.io.IOException;

/**
 * @author Stefan Hufschmidt
 */
public class MSSQLServerContainer<SELF extends MSSQLServerContainer<SELF>> extends JdbcDatabaseContainer<SELF> {
    static final String NAME = "mssqlserver";
    static final String IMAGE = "microsoft/mssql-server-linux";
    public static final Integer MS_SQL_SERVER_PORT = 1433;
    private String username = "SA";
    private String password = "A_Str0ng_Required_Password";
    private String dbName;
    private String collation;

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

    @Override
    protected void waitUntilContainerStarted() {
        getWaitStrategy().waitUntilReady(this);
    }

    @Override
    public SELF withDatabaseName(final String dbName) {
        this.dbName = dbName;
        return self();
    }

    /**
     * Sets the collation for a database which is specified by {@link #withDatabaseName(String)}.
     * The collation will only be set if a database has been specified by {@link #withDatabaseName(String)} before the container start.
     * It will only be set at the one database which will be created.
     *
     * @param collation the collation to set at the database
     * @return self
     */
    public SELF withCollation(final String collation) {
        this.collation = collation;
        return self();
    }

    @Override
    public void start() {
        super.start();

        if (dbName != null) {
            createDatabase();
        }
    }

    /**
     * Creates a database in MS SQL Server by using the command line tool 'sqlcmd' inside the MS SQL Server container.
     * The database will get the name specified by {@link #withDatabaseName(String)}.
     * If a collation is set by {@link #withCollation(String)}, it will be applied to the database which will be created.
     */
    private void createDatabase() {
        StringBuilder createQueryBuilder = new StringBuilder("CREATE DATABASE ")
                .append(dbName);

        if (collation != null) {
            createQueryBuilder.append(" COLLATE ")
                    .append(collation);
        }

        try {
            execInContainer(
                    "/opt/mssql-tools/bin/sqlcmd",
                    "-S", "localhost",
                    "-U", username,
                    "-P", password,
                    "-Q", createQueryBuilder.toString()
            );
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
