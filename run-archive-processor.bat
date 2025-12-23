@echo off
echo Starting Blob Archive Processor...
REM Change to script directory to find config files
cd /d "%~dp0"
java -cp target\blob-util-1.0.0.jar com.dtc.blobutil.BlobArchiveProcessor my-config.conf
pause





