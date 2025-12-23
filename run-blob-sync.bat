@echo off
echo Starting Blob Change Feed Sync...
java -cp target\blob-util-1.0.0.jar com.dtc.blobutil.BlobChangeFeedSync my-config.conf
pause





