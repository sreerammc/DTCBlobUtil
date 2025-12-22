package com.dtc.blobutil.config;

/**
 * Configuration for InfluxDB 3 / FlightSQL connection
 */
public class InfluxConfig {
    private String host;
    private int port;
    private String database;
    private String token;
    private String queryTemplate;
    private boolean skipTlsValidation;
    private String protocol; // "grpc", "http", or "https"
    private boolean useHttps; // Deprecated: use protocol instead

    public InfluxConfig() {
        this.protocol = "grpc"; // Default to gRPC (FlightSQL)
        this.skipTlsValidation = false;
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

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    /**
     * SQL query template with a single %s placeholder for the file name.
     * For example:
     * SELECT count(*) FROM iris_data
     *   WHERE time >= NOW() - INTERVAL '1 day'
     *     AND file_name = '%s'
     */
    public String getQueryTemplate() {
        return queryTemplate;
    }

    public void setQueryTemplate(String queryTemplate) {
        this.queryTemplate = queryTemplate;
    }

    /**
     * If true, connect without TLS (insecure). Only use in non-production environments.
     */
    public boolean isSkipTlsValidation() {
        return skipTlsValidation;
    }

    public void setSkipTlsValidation(boolean skipTlsValidation) {
        this.skipTlsValidation = skipTlsValidation;
    }

    /**
     * Protocol to use for InfluxDB connection.
     * Valid values: "grpc" (FlightSQL), "http", or "https"
     * Default: "grpc"
     */
    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        if (protocol != null && !protocol.isEmpty()) {
            String lower = protocol.toLowerCase();
            if (lower.equals("grpc") || lower.equals("http") || lower.equals("https")) {
                this.protocol = lower;
            } else {
                throw new IllegalArgumentException("Protocol must be 'grpc', 'http', or 'https'. Got: " + protocol);
            }
        }
    }

    /**
     * @deprecated Use getProtocol() instead. Returns true if protocol is "https"
     */
    @Deprecated
    public boolean isUseHttps() {
        return "https".equalsIgnoreCase(protocol);
    }

    /**
     * @deprecated Use setProtocol("https") or setProtocol("http") instead
     */
    @Deprecated
    public void setUseHttps(boolean useHttps) {
        this.protocol = useHttps ? "https" : "http";
    }

    /**
     * Check if using HTTP/HTTPS protocol (not gRPC)
     */
    public boolean isHttpProtocol() {
        return "http".equalsIgnoreCase(protocol) || "https".equalsIgnoreCase(protocol);
    }

    /**
     * Get the full URL for HTTP/HTTPS connections
     */
    public String getHttpUrl() {
        String scheme = "https".equalsIgnoreCase(protocol) ? "https" : "http";
        return String.format("%s://%s:%d", scheme, host, port);
    }
}



