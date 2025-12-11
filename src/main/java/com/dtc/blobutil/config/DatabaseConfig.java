package com.dtc.blobutil.config;

/**
 * Configuration for PostgreSQL database connection
 */
public class DatabaseConfig {
    private String host;
    private int port;
    private String database;
    private String username;
    private String password;
    private String schema;
    private String tableName;
    private int maxPoolSize;
    private boolean ssl;
    private String sslMode; // disable, allow, prefer, require, verify-ca, verify-full

    public DatabaseConfig() {
        this.port = 5432;
        this.maxPoolSize = 10;
        this.schema = "public";
        this.tableName = "blob_changes";
        this.ssl = false;
        this.sslMode = "disable";  // Default to disable SSL for local connections
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public String getSslMode() {
        return sslMode;
    }

    public void setSslMode(String sslMode) {
        this.sslMode = sslMode;
    }

    public String getJdbcUrl() {
        StringBuilder url = new StringBuilder(String.format("jdbc:postgresql://%s:%d/%s", host, port, database));
        
        // Add connection parameters
        boolean hasParams = false;
        if (sslMode != null && !sslMode.isEmpty()) {
            url.append("?sslmode=").append(sslMode);
            hasParams = true;
        }
        if (ssl) {
            url.append(hasParams ? "&ssl=true" : "?ssl=true");
            hasParams = true;
        }
        
        return url.toString();
    }
}


