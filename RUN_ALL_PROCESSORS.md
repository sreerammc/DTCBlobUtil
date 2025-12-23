# Running All Three Processors

This utility has three main processors that can run independently or together:

1. **BlobChangeFeedSync** - Syncs blob changes from Azure Blob Storage to PostgreSQL
2. **BlobArchiveProcessor** - Processes archived blob files and updates record counts
3. **InfluxVerificationProcessor** - Verifies archived data in InfluxDB

## Running Individual Processors

Since all three are in the same JAR file, you need to specify the main class:

### 1. Blob Change Feed Sync
```bash
java -cp target/blob-util-1.0.0.jar com.dtc.blobutil.BlobChangeFeedSync my-config.conf
```

### 2. Blob Archive Processor
```bash
java -cp target/blob-util-1.0.0.jar com.dtc.blobutil.BlobArchiveProcessor my-config.conf
```

### 3. Influx Verification Processor
```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED -cp target/blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor my-config.conf
```

**Note:** The `--add-opens` JVM argument is required for Java 9+ when using Apache Arrow (used by InfluxDB 3 FlightSQL client). This exposes JDK internals needed by Arrow for memory management.

## Running All Three Together

You have several options:

### Option 1: Run in Separate Terminal Windows (Recommended)
Open three separate terminal windows and run each processor:

**Terminal 1:**
```bash
java -cp target/blob-util-1.0.0.jar com.dtc.blobutil.BlobChangeFeedSync my-config.conf
```

**Terminal 2:**
```bash
java -cp target/blob-util-1.0.0.jar com.dtc.blobutil.BlobArchiveProcessor my-config.conf
```

**Terminal 3:**
```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED -cp target/blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor my-config.conf
```

### Option 2: Run in Background (Windows PowerShell)
```powershell
Start-Process java -ArgumentList "-cp","target/blob-util-1.0.0.jar","com.dtc.blobutil.BlobChangeFeedSync","my-config.conf" -WindowStyle Hidden
Start-Process java -ArgumentList "-cp","target/blob-util-1.0.0.jar","com.dtc.blobutil.BlobArchiveProcessor","my-config.conf" -WindowStyle Hidden
Start-Process java -ArgumentList "-cp","target/blob-util-1.0.0.jar","com.dtc.blobutil.InfluxVerificationProcessor","my-config.conf" -WindowStyle Hidden
```

### Option 3: Create Batch Scripts

Create three batch files:

**run-blob-sync.bat:**
```batch
@echo off
java -cp target\blob-util-1.0.0.jar com.dtc.blobutil.BlobChangeFeedSync my-config.conf
pause
```

**run-archive-processor.bat:**
```batch
@echo off
java -cp target\blob-util-1.0.0.jar com.dtc.blobutil.BlobArchiveProcessor my-config.conf
pause
```

**run-influx-verification.bat:**
```batch
@echo off
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp target\blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor my-config.conf
pause
```

**run-all.bat:**
```batch
@echo off
echo Starting all three processors...
start "Blob Change Feed Sync" cmd /k "java -cp target\blob-util-1.0.0.jar com.dtc.blobutil.BlobChangeFeedSync my-config.conf"
timeout /t 2 /nobreak >nul
start "Blob Archive Processor" cmd /k "java -cp target\blob-util-1.0.0.jar com.dtc.blobutil.BlobArchiveProcessor my-config.conf"
timeout /t 2 /nobreak >nul
start "Influx Verification Processor" cmd /k "java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp target\blob-util-1.0.0.jar com.dtc.blobutil.InfluxVerificationProcessor my-config.conf"
echo All processors started in separate windows.
pause
```

## Using -jar Option (Alternative)

If you prefer using `-jar` instead of `-cp`, you can also run:

```bash
java -jar target/blob-util-1.0.0.jar com.dtc.blobutil.BlobChangeFeedSync my-config.conf
```

However, this requires the JAR to have the main class in the manifest, which currently only has `BlobChangeFeedSync`.

## Process Flow

The typical workflow is:

1. **BlobChangeFeedSync** - Monitors blob storage for changes and syncs to PostgreSQL
2. **BlobArchiveProcessor** - Processes archived files and updates record counts
3. **InfluxVerificationProcessor** - Verifies the archived data exists in InfluxDB

All three can run simultaneously as they work on different aspects of the pipeline.



