package com.dtc.blobutil.influx;

import com.dtc.blobutil.config.InfluxConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;

/**
 * HTTP/HTTPS client for InfluxDB 3 query API
 */
public class InfluxHttpClient implements InfluxClient {
    private static final Logger logger = LoggerFactory.getLogger(InfluxHttpClient.class);
    private final InfluxConfig config;
    private final ObjectMapper objectMapper;
    private final boolean skipTlsValidation;

    public InfluxHttpClient(InfluxConfig config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
        this.skipTlsValidation = config.isSkipTlsValidation();
        
        if (skipTlsValidation && config.isHttpProtocol()) {
            logger.warn("Influx HTTP: skipTlsValidation=true, using insecure SSL connection");
            setupInsecureSsl();
        }
    }

    /**
     * Execute a SQL query and return count result
     */
    public long queryCount(String sql) throws Exception {
        String url = buildQueryUrl(sql);
        logger.debug("Executing Influx HTTP query: {} to {}", sql, url);

        HttpURLConnection conn = createConnection(url);
        try {
            // Use GET method (as per curl example)
            int responseCode = conn.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                String errorBody = readErrorResponse(conn);
                throw new IOException("InfluxDB HTTP query failed with code " + responseCode + ": " + errorBody);
            }

            // Parse JSON response - InfluxDB 3 returns a JSON array directly
            JsonNode response = objectMapper.readTree(conn.getInputStream());
            return extractCountFromResponse(response);
        } finally {
            conn.disconnect();
        }
    }

    private String buildQueryUrl(String sql) throws Exception {
        String baseUrl = config.getHttpUrl();
        // InfluxDB 3 query endpoint (matches curl: /api/v3/query_sql)
        String endpoint = baseUrl + "/api/v3/query_sql";
        
        // Build query string with URL-encoded parameters
        String encodedDb = java.net.URLEncoder.encode(config.getDatabase(), StandardCharsets.UTF_8.toString());
        String encodedSql = java.net.URLEncoder.encode(sql, StandardCharsets.UTF_8.toString());
        
        return endpoint + "?db=" + encodedDb + "&q=" + encodedSql;
    }

    private HttpURLConnection createConnection(String urlString) throws IOException {
        URL url;
        try {
            url = new URL(urlString);
        } catch (java.net.MalformedURLException e) {
            throw new IOException("Invalid InfluxDB URL: " + urlString, e);
        }
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        
        // Use GET method (as per curl example)
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Bearer " + config.getToken());
        conn.setRequestProperty("Accept", "application/json");
        
        conn.setConnectTimeout(30000);
        conn.setReadTimeout(60000);
        
        return conn;
    }

    private long extractCountFromResponse(JsonNode response) throws Exception {
        logger.debug("Parsing InfluxDB response: {}", response.toString());
        
        // InfluxDB 3 /api/v3/query_sql returns a JSON array directly
        // For count(*) queries, response format: [{"count(*)": 5}] or [{"count": 5}]
        
        if (response.isArray()) {
            if (response.size() == 0) {
                logger.debug("Empty array response, returning count 0");
                return 0L;
            }
            
            // For count(*) queries, typically returns array with one object containing count
            JsonNode firstItem = response.get(0);
            if (firstItem.isObject()) {
                // Look for "count(*)" field first (exact match from InfluxDB 3)
                if (firstItem.has("count(*)")) {
                    long count = firstItem.get("count(*)").asLong();
                    logger.info("Extracted count from 'count(*)' field: {}", count);
                    return count;
                }
                
                // Look for "count" field (simpler variant)
                if (firstItem.has("count")) {
                    long count = firstItem.get("count").asLong();
                    logger.info("Extracted count from 'count' field: {}", count);
                    return count;
                }
                
                // Look for any numeric field (some queries might return count in different field)
                // For count(*), the result might be in a field named after the column
                java.util.Iterator<String> fieldNames = firstItem.fieldNames();
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    JsonNode fieldValue = firstItem.get(fieldName);
                    if (fieldValue.isNumber()) {
                        long count = fieldValue.asLong();
                        logger.info("Extracted count from field '{}': {}", fieldName, count);
                        return count;
                    }
                }
                
                // Collect field names for error message
                java.util.List<String> fieldNamesList = new java.util.ArrayList<>();
                java.util.Iterator<String> fieldNamesIter = firstItem.fieldNames();
                while (fieldNamesIter.hasNext()) {
                    fieldNamesList.add(fieldNamesIter.next());
                }
                logger.error("No numeric field found in response object. Available fields: {}", 
                    String.join(", ", fieldNamesList));
            }
            
            // If first item is a number directly
            if (firstItem.isNumber()) {
                long count = firstItem.asLong();
                logger.info("Extracted count as direct number: {}", count);
                return count;
            }
            
            logger.error("Could not extract count from array response. First item type: {}, content: {}", 
                firstItem.getNodeType(), firstItem.toString());
            throw new IllegalStateException("Could not extract count from array response. Expected count(*) to return a count value. Response: " + response.toString());
        }
        
        // If it's an object, check for count field
        if (response.isObject()) {
            if (response.has("count")) {
                long count = response.get("count").asLong();
                logger.debug("Extracted count from object 'count' field: {}", count);
                return count;
            }
        }
        
        // Check for results/series pattern (InfluxQL format - legacy)
        if (response.has("results")) {
            JsonNode results = response.get("results");
            if (results.isArray() && results.size() > 0) {
                JsonNode firstResult = results.get(0);
                if (firstResult.has("series")) {
                    JsonNode series = firstResult.get("series");
                    if (series.isArray() && series.size() > 0) {
                        JsonNode firstSeries = series.get(0);
                        if (firstSeries.has("values")) {
                            JsonNode values = firstSeries.get("values");
                            if (values.isArray() && values.size() > 0) {
                                JsonNode firstRow = values.get(0);
                                if (firstRow.isArray() && firstRow.size() > 0) {
                                    long count = firstRow.get(0).asLong();
                                    logger.debug("Extracted count from InfluxQL format: {}", count);
                                    return count;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        logger.error("Could not extract count from response. Response type: {}, Content: {}", 
            response.getNodeType(), response.toString());
        throw new IllegalStateException("Could not extract count from InfluxDB response: " + response.toString());
    }

    private String readErrorResponse(HttpURLConnection conn) {
        try {
            if (conn.getErrorStream() != null) {
                byte[] buffer = new byte[1024];
                int bytesRead = conn.getErrorStream().read(buffer);
                if (bytesRead > 0) {
                    return new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to read error response", e);
        }
        return "No error details available";
    }

    private void setupInsecureSsl() {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() { return null; }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) { }
                    public void checkServerTrusted(X509Certificate[] certs, String authType) { }
                }
            };
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            SSLContext.setDefault(sslContext);
        } catch (Exception e) {
            logger.error("Failed to setup insecure SSL", e);
        }
    }

    @Override
    public void close() {
        // HTTP client doesn't need explicit cleanup
    }
}

