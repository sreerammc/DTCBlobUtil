package com.dtc.blobutil.influx;

import com.dtc.blobutil.config.InfluxConfig;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Small helper wrapper around Apache Arrow Flight SQL client to query InfluxDB 3.
 */
public class InfluxFlightClient implements InfluxClient {
    private static final Logger logger = LoggerFactory.getLogger(InfluxFlightClient.class);

    private final BufferAllocator allocator;
    private final FlightClient client;
    private final FlightSqlClient sqlClient;
    private final CredentialCallOption auth;

    public InfluxFlightClient(InfluxConfig cfg) {
        this.allocator = new RootAllocator(Long.MAX_VALUE);

        // If skipTlsValidation=true, connect without TLS (insecure). Only for dev/test.
        // Note: FlightSQL typically uses a different port than HTTP REST API
        // Common ports: 443 (TLS), 8082 (insecure), or custom ports
        final Location location;
        if (cfg.isSkipTlsValidation()) {
            logger.warn("Influx FlightSQL: skipTlsValidation=true, using insecure (non-TLS) connection to {}:{}",
                cfg.getHost(), cfg.getPort());
            logger.info("Note: FlightSQL typically uses port 8082 for insecure connections. If you get connection errors, try port 8082.");
            location = Location.forGrpcInsecure(cfg.getHost(), cfg.getPort());
        } else {
            logger.info("Influx FlightSQL: using TLS connection to {}:{}", cfg.getHost(), cfg.getPort());
            logger.info("Note: FlightSQL typically uses port 443 for TLS connections. If you get connection errors, try port 443.");
            location = Location.forGrpcTls(cfg.getHost(), cfg.getPort());
        }

        // Create bearer credential using token (as per InfluxDB 3 Core documentation)
        this.auth = new CredentialCallOption(new BearerCredentialWriter(cfg.getToken()));

        // Add "database" header for InfluxDB 3 (authorization is handled via CredentialCallOption)
        FlightClientMiddleware.Factory dbHeaderFactory = info -> new FlightClientMiddleware() {
            @Override
            public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                outgoingHeaders.insert("database", cfg.getDatabase());
            }

            @Override
            public void onHeadersReceived(CallHeaders incomingHeaders) {
                // no-op
            }

            @Override
            public void onCallCompleted(CallStatus status) {
                // no-op
            }
        };

        this.client = FlightClient.builder(allocator, location)
            .intercept(dbHeaderFactory)
            .build();
        this.sqlClient = new FlightSqlClient(client);
    }

    /**
     * Execute a SQL query expected to return a single row with a single BIGINT column (count(*)).
     */
    public long queryCount(String sql) throws Exception {
        logger.debug("Executing Influx FlightSQL query: {}", sql);
        // Execute query with CredentialCallOption (as per InfluxDB 3 Core documentation)
        FlightInfo flightInfo = sqlClient.execute(sql, auth);

        if (flightInfo.getEndpoints().isEmpty()) {
            throw new IllegalStateException("No FlightSQL endpoints returned for query");
        }

        // Simple case: first endpoint/ticket
        FlightEndpoint endpoint = flightInfo.getEndpoints().get(0);

        // Get stream with CredentialCallOption (as per InfluxDB 3 Core documentation)
        try (FlightStream stream = sqlClient.getStream(endpoint.getTicket(), auth)) {
            long count = 0L;
            boolean found = false;

            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                if (root.getRowCount() == 0 || root.getFieldVectors().isEmpty()) {
                    continue;
                }
                // Assume count(*) is column 0, row 0
                if (root.getFieldVectors().get(0) instanceof BigIntVector) {
                    BigIntVector v = (BigIntVector) root.getFieldVectors().get(0);
                    if (v.getValueCount() > 0) {
                        count = v.get(0);
                        found = true;
                        break;
                    }
                } else {
                    throw new IllegalStateException("Expected BIGINT column for count(*), got: "
                        + root.getSchema().getFields().get(0).getType());
                }
            }

            if (!found) {
                throw new IllegalStateException("No rows returned for count(*) query");
            }

            logger.debug("Influx FlightSQL count(*) result: {}", count);
            return count;
        }
    }

    @Override
    public void close() {
        try {
            sqlClient.close();
        } catch (Exception ignore) {
        }
        try {
            client.close();
        } catch (Exception ignore) {
        }
        try {
            allocator.close();
        } catch (Exception ignore) {
        }
    }
}

