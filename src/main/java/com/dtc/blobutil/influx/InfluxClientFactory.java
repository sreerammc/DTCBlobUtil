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
        logger.info("Creating InfluxDB client with protocol: '{}' (host: {}, port: {})", 
            protocol, config.getHost(), config.getPort());
        
        logger.debug("Protocol from config object: '{}' (null: {}, empty: {})", 
            protocol, protocol == null, protocol != null && protocol.isEmpty());

        if (protocol == null || protocol.isEmpty()) {
            logger.warn("Protocol is null or empty, defaulting to 'grpc'");
            protocol = "grpc";
        }
        
        String protocolLower = protocol.toLowerCase().trim();
        logger.debug("Normalized protocol: '{}'", protocolLower);
        
        if ("grpc".equals(protocolLower)) {
            logger.info("Using InfluxFlightClient (FlightSQL/gRPC) for protocol: {}", protocol);
            return new InfluxFlightClient(config);
        } else if ("http".equals(protocolLower) || "https".equals(protocolLower)) {
            logger.info("Using InfluxHttpClient (HTTP REST API) for protocol: {}", protocol);
            return new InfluxHttpClient(config);
        } else {
            throw new IllegalArgumentException("Unsupported InfluxDB protocol: '" + protocol + 
                "'. Must be 'grpc', 'http', or 'https'");
        }
    }
}





