package com.dtc.blobutil.influx;

/**
 * Interface for InfluxDB clients (both FlightSQL and HTTP)
 */
public interface InfluxClient extends AutoCloseable {
    /**
     * Execute a SQL query and return count result
     */
    long queryCount(String sql) throws Exception;
}





