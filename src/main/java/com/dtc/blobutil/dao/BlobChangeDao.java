package com.dtc.blobutil.dao;

import com.dtc.blobutil.model.BlobChangeEvent;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Data Access Object for managing blob change events in PostgreSQL
 */
public class BlobChangeDao {
    private static final Logger logger = LoggerFactory.getLogger(BlobChangeDao.class);
    private final DataSource dataSource;
    private final String tableName;
    private final String schema;

    public BlobChangeDao(DataSource dataSource, String schema, String tableName) {
        this.dataSource = dataSource;
        this.schema = schema;
        this.tableName = tableName;
    }

    /**
     * Initialize the database table if it doesn't exist
     */
    public void initializeTable() throws SQLException {
        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (" +
            "id BIGSERIAL PRIMARY KEY, " +
            "blob_name VARCHAR(1024) NOT NULL, " +
            "event_type VARCHAR(100) NOT NULL, " +
            "content_type VARCHAR(255), " +
            "content_length BIGINT, " +
            "etag VARCHAR(255), " +
            "last_modified TIMESTAMP WITH TIME ZONE, " +
            "metadata JSONB, " +
            "url TEXT, " +
            "version_id VARCHAR(255), " +
            "snapshot VARCHAR(255), " +
            "previous_info TEXT, " +
            "created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, " +
            "UNIQUE(blob_name, event_type, last_modified)" +
            ");",
            schema, tableName
        );

        String createIndexSql = String.format(
            "CREATE INDEX IF NOT EXISTS idx_%s_blob_name ON %s.%s(blob_name);",
            tableName, schema, tableName
        );

        String createIndexTimeSql = String.format(
            "CREATE INDEX IF NOT EXISTS idx_%s_last_modified ON %s.%s(last_modified);",
            tableName, schema, tableName
        );

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute(createTableSql);
            stmt.execute(createIndexSql);
            stmt.execute(createIndexTimeSql);
            logger.info("Table {} initialized successfully", tableName);
        }
    }

    /**
     * Upsert a blob change event into the database
     */
    public void upsertBlobChange(BlobChangeEvent event) throws SQLException {
        String sql = String.format(
            "INSERT INTO %s.%s (blob_name, event_type, content_type, content_length, etag, " +
            "last_modified, metadata, url, version_id, snapshot, previous_info) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?) " +
            "ON CONFLICT (blob_name, event_type, last_modified) " +
            "DO UPDATE SET " +
            "content_type = EXCLUDED.content_type, " +
            "content_length = EXCLUDED.content_length, " +
            "etag = EXCLUDED.etag, " +
            "metadata = EXCLUDED.metadata, " +
            "url = EXCLUDED.url, " +
            "version_id = EXCLUDED.version_id, " +
            "snapshot = EXCLUDED.snapshot, " +
            "previous_info = EXCLUDED.previous_info",
            schema, tableName
        );

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            pstmt.setString(1, event.getBlobName());
            pstmt.setString(2, event.getEventType());
            pstmt.setString(3, event.getContentType());
            pstmt.setObject(4, event.getContentLength(), Types.BIGINT);
            pstmt.setString(5, event.getEtag());
            pstmt.setObject(6, event.getLastModified(), Types.TIMESTAMP_WITH_TIMEZONE);
            
            // Convert metadata map to JSON string
            String metadataJson = event.getMetadata() != null && !event.getMetadata().isEmpty()
                ? convertMetadataToJson(event.getMetadata())
                : null;
            pstmt.setString(7, metadataJson);
            
            pstmt.setString(8, event.getUrl());
            pstmt.setString(9, event.getVersionId());
            pstmt.setString(10, event.getSnapshot());
            pstmt.setString(11, event.getPreviousInfo());

            pstmt.executeUpdate();
            logger.debug("Upserted blob change event: {}", event.getBlobName());
        }
    }

    /**
     * Convert metadata map to JSON string
     */
    private String convertMetadataToJson(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return "{}";
        }
        return "{" + metadata.entrySet().stream()
            .map(e -> "\"" + escapeJson(e.getKey()) + "\":\"" + escapeJson(e.getValue()) + "\"")
            .collect(Collectors.joining(",")) + "}";
    }

    private String escapeJson(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\")
                  .replace("\"", "\\\"")
                  .replace("\n", "\\n")
                  .replace("\r", "\\r")
                  .replace("\t", "\\t");
    }

    /**
     * Get the last processed timestamp for resuming change feed processing
     */
    public OffsetDateTime getLastProcessedTimestamp() throws SQLException {
        String sql = String.format(
            "SELECT MAX(last_modified) FROM %s.%s",
            schema, tableName
        );

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            if (rs.next() && rs.getObject(1) != null) {
                return rs.getObject(1, OffsetDateTime.class);
            }
            return null;
        }
    }

    /**
     * Create a HikariCP DataSource
     */
    public static DataSource createDataSource(String jdbcUrl, String username, String password, int maxPoolSize) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(maxPoolSize);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);
        config.setLeakDetectionThreshold(60000);
        
        // Add connection test query for better diagnostics
        config.setConnectionTestQuery("SELECT 1");
        
        // Log connection details (without password)
        logger.info("Creating database connection pool:");
        logger.info("  JDBC URL: {}", jdbcUrl);
        logger.info("  Username: {}", username);
        logger.info("  Max Pool Size: {}", maxPoolSize);
        
        try {
            HikariDataSource dataSource = new HikariDataSource(config);
            // Test the connection
            try (java.sql.Connection conn = dataSource.getConnection()) {
                logger.info("Successfully connected to PostgreSQL database");
            }
            return dataSource;
        } catch (Exception e) {
            logger.error("Failed to create database connection pool", e);
            logger.error("Connection details: host={}, port={}, database={}, username={}", 
                extractHost(jdbcUrl), extractPort(jdbcUrl), extractDatabase(jdbcUrl), username);
            throw new RuntimeException("Failed to connect to PostgreSQL database. Please check:", e);
        }
    }
    
    private static String extractHost(String jdbcUrl) {
        try {
            int start = jdbcUrl.indexOf("//") + 2;
            int end = jdbcUrl.indexOf(":", start);
            return end > start ? jdbcUrl.substring(start, end) : "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }
    
    private static String extractPort(String jdbcUrl) {
        try {
            int start = jdbcUrl.indexOf("//") + 2;
            int portStart = jdbcUrl.indexOf(":", start) + 1;
            int portEnd = jdbcUrl.indexOf("/", portStart);
            return portEnd > portStart ? jdbcUrl.substring(portStart, portEnd) : "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }
    
    private static String extractDatabase(String jdbcUrl) {
        try {
            int start = jdbcUrl.lastIndexOf("/") + 1;
            int end = jdbcUrl.indexOf("?", start);
            return end > start ? jdbcUrl.substring(start, end) : jdbcUrl.substring(start);
        } catch (Exception e) {
            return "unknown";
        }
    }
}


