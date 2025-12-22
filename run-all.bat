@echo off
echo ========================================
echo Starting All Three Blob Utility Processors
echo ========================================
echo.

echo [1/3] Starting Blob Change Feed Sync...
start "Blob Change Feed Sync" cmd /k "java -cp target\blob-util-1.0.0.jar com.dtc.blobutil.BlobChangeFeedSync my-config.conf"
timeout /t 2 /nobreak >nul

echo [2/3] Starting Blob Archive Processor...
start "Blob Archive Processor" cmd /k "java -cp target\blob-util-1.0.0.jar com.dtc.blobutil.BlobArchiveProcessor my-config.conf"
timeout /t 2 /nobreak >nul

echo [3/3] Starting Influx Verification Processor...
start "Influx Verification Processor" cmd /k "java -cp target\blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor my-config.conf"
timeout /t 2 /nobreak >nul

echo.
echo ========================================
echo All three processors started successfully!
echo ========================================
echo.
echo Each processor is running in its own window.
echo Close the windows or press Ctrl+C in each to stop.
echo.
pause




