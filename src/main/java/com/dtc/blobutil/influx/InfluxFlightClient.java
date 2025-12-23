package com.dtc.blobutil.influx;

import com.dtc.blobutil.config.InfluxConfig;
import com.influxdb.v3.client.InfluxDBClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * InfluxDB 3 FlightSQL client using the official InfluxDB 3 Java client library.
 * Uses com.influxdb.v3.client.InfluxDBClient which wraps Apache Arrow Flight SQL.
 * 
 * Documentation: https://docs.influxdata.com/influxdb3/clustered/reference/client-libraries/v3/java/
 */
public class InfluxFlightClient implements InfluxClient {
    private static final Logger logger = LoggerFactory.getLogger(InfluxFlightClient.class);

    private final InfluxDBClient client;
    private final InfluxConfig config;

    public InfluxFlightClient(InfluxConfig cfg) {
        this.config = cfg;

        // Validate that protocol is actually "grpc" - FlightSQL should not be used for HTTP/HTTPS
        String protocol = cfg.getProtocol();
        if (!"grpc".equalsIgnoreCase(protocol)) {
            throw new IllegalArgumentException(
                "InfluxFlightClient should only be used with protocol='grpc'. " +
                "Current protocol is '" + protocol + "'. " +
                "For HTTP/HTTPS connections, use InfluxHttpClient instead. " +
                "Check your configuration file and ensure influx.protocol is set correctly.");
        }

        // Build host URL - official client expects full URL with protocol
        String hostUrl = buildHostUrl(cfg);
        
        logger.info("Initializing InfluxDB 3 official client (FlightSQL/gRPC):");
        logger.info("  Host URL: {}", hostUrl);
        logger.info("  Database: {}", cfg.getDatabase());
        logger.info("  Skip TLS Validation: {}", cfg.isSkipTlsValidation());
        
        if (cfg.isSkipTlsValidation()) {
            logger.warn("Influx FlightSQL: skipTlsValidation=true, using insecure SSL connection (not recommended for production)");
            logger.info("Note: FlightSQL typically uses port 8082 for insecure connections. If you get connection errors, try port 8082.");
        } else {
            logger.info("Influx FlightSQL: using TLS connection");
            logger.info("Note: FlightSQL typically uses port 443 for TLS connections. If you get connection errors, try port 443.");
        }

        // Convert token string to char array (as required by official client)
        char[] tokenChars = cfg.getToken().toCharArray();
        
        // Initialize official InfluxDB 3 client
        // Documentation: https://docs.influxdata.com/influxdb3/clustered/reference/client-libraries/v3/java/
        this.client = InfluxDBClient.getInstance(hostUrl, tokenChars, cfg.getDatabase());
    }

    /**
     * Build the full host URL for the official client.
     * The official client expects: https://host:port or http://host:port
     */
    private String buildHostUrl(InfluxConfig cfg) {
        String scheme = cfg.isSkipTlsValidation() ? "http" : "https";
        return String.format("%s://%s:%d", scheme, cfg.getHost(), cfg.getPort());
    }

    /**
     * Execute a SQL query expected to return a single row with a single BIGINT column (count(*)).
     * Uses the official InfluxDB 3 client's query() method which returns Stream<Object[]>.
     */
    public long queryCount(String sql) throws Exception {
        logger.debug("Executing Influx FlightSQL query using official client: {}", sql);
        
        try {
            // Official client's query() method returns Stream<Object[]>
            // Each Object[] represents a row, with columns in order
            try (Stream<Object[]> stream = client.query(sql)) {
                // For count(*) queries, we expect a single row with a single numeric value
                Object[] firstRow = stream.findFirst()
                    .orElseThrow(() -> new IllegalStateException("No rows returned for count(*) query"));
                
                if (firstRow.length == 0) {
                    throw new IllegalStateException("Query returned row with no columns");
                }
                
                // Extract count from first column (count(*) is typically the first/only column)
                Object countValue = firstRow[0];
                long count;
                
                if (countValue instanceof Number) {
                    count = ((Number) countValue).longValue();
                } else if (countValue instanceof String) {
                    // Try to parse as long if it's a string
                    count = Long.parseLong((String) countValue);
                } else {
                    throw new IllegalStateException(
                        "Expected numeric value for count(*), got: " + 
                        (countValue != null ? countValue.getClass().getName() : "null"));
                }
                
                logger.debug("Influx FlightSQL count(*) result: {}", count);
                return count;
            }
        } catch (Exception e) {
            // Provide helpful error messages for common issues
            Throwable cause = e.getCause();
            if (cause != null && cause.getMessage() != null) {
                String message = cause.getMessage();
                if (message.contains("First received frame was not SETTINGS") || 
                    message.contains("HTTP/") || 
                    message.contains("http2")) {
                    throw new RuntimeException(
                        "FlightSQL (gRPC/HTTP2) connection failed. The server appears to be responding with HTTP/1.1 instead of HTTP/2. " +
                        "This usually means:\n" +
                        "1. You're trying to connect to an HTTP REST API endpoint instead of a FlightSQL endpoint\n" +
                        "2. Your configuration has protocol='http' or 'https' but FlightSQL client is being used\n" +
                        "3. The port you're using is for HTTP REST API, not FlightSQL\n\n" +
                        "Solution: Check your configuration file and ensure:\n" +
                        "- For HTTP REST API: set influx.protocol = \"http\" or \"https\"\n" +
                        "- For FlightSQL: set influx.protocol = \"grpc\" and use the correct FlightSQL port (typically 443 for TLS, 8082 for insecure)\n" +
                        "If you see this error, verify that InfluxClientFactory is correctly selecting the client type based on your protocol setting.",
                        e);
                }
            }
            throw e;
        }
    }

    @Override
    public void close() {
        try {
            // Official client implements AutoCloseable
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing InfluxDB client", e);
        }
    }
}

