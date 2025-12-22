package com.dtc.blobutil.influx;

import com.dtc.blobutil.config.InfluxConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to create appropriate InfluxDB client based on configuration
 */
public class InfluxClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(InfluxClientFactory.class);

    public static InfluxClient create(InfluxConfig config) {
        String protocol = config.getProtocol();
        logger.info("Creating InfluxDB client with protocol: {}", protocol);

        if ("grpc".equalsIgnoreCase(protocol)) {
            return new InfluxFlightClient(config);
        } else if ("http".equalsIgnoreCase(protocol) || "https".equalsIgnoreCase(protocol)) {
            return new InfluxHttpClient(config);
        } else {
            throw new IllegalArgumentException("Unsupported InfluxDB protocol: " + protocol + 
                ". Must be 'grpc', 'http', or 'https'");
        }
    }
}




