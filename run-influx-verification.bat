@echo off
echo Starting Influx Verification Processor...
REM Change to script directory to find config files
cd /d "%~dp0"
java --add-opens=java.base/java.nio=ALL-UNNAMED -cp target\blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor my-config.conf
pause





