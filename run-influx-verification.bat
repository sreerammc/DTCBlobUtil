@echo off
echo Starting Influx Verification Processor...
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp target\blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor my-config.conf
pause





