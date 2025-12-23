@echo off
echo Starting Influx Verification Processor...
java -cp target\blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor my-config.conf
pause





