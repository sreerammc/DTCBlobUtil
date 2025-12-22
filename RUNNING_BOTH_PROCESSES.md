# Running Both Processes Independently

This project contains two independent processes that can run simultaneously:

1. **BlobChangeFeedSync** - Monitors `containerName` and writes blob change events to database
2. **BlobArchiveProcessor** - Processes files from `archiveContainerName` older than 10 minutes and updates record counts

## Building the Project

First, build the project to create both executable JARs:

```bash
mvn clean package
```

This will create two JAR files in the `target` directory:
- `blob-util-1.0.0.jar` - For BlobChangeFeedSync
- `blob-util-archive-1.0.0.jar` - For BlobArchiveProcessor

## Running the Processes

### Option 1: Using Separate Executable JARs (Recommended)

#### Terminal 1 - Run BlobChangeFeedSync (Original Process)
```bash
java -jar target/blob-util-1.0.0.jar my-config.conf
```

This process will:
- Monitor the `containerName` container
- Detect new blob changes
- Write blob metadata to the database

#### Terminal 2 - Run BlobArchiveProcessor (New Process)
```bash
java -jar target/blob-util-archive-1.0.0.jar my-config.conf
```

This process will:
- Query database for files older than 10 minutes
- Read files from `archiveContainerName` container
- Parse JSON and count records
- Update `total_records` and `distinct_records` in database

### Option 2: Using the Same JAR with Different Main Classes

If you only have one JAR file, you can specify the main class:

#### Terminal 1 - Run BlobChangeFeedSync
```bash
java -cp target/blob-util-1.0.0.jar com.dtc.blobutil.BlobChangeFeedSync my-config.conf
```

#### Terminal 2 - Run BlobArchiveProcessor
```bash
java -cp target/blob-util-1.0.0.jar com.dtc.blobutil.BlobArchiveProcessor my-config.conf
```

## Running as Background Services

### Windows (PowerShell)

#### Terminal 1 - BlobChangeFeedSync
```powershell
Start-Process java -ArgumentList "-jar","target/blob-util-1.0.0.jar","my-config.conf" -WindowStyle Hidden
```

#### Terminal 2 - BlobArchiveProcessor
```powershell
Start-Process java -ArgumentList "-jar","target/blob-util-archive-1.0.0.jar","my-config.conf" -WindowStyle Hidden
```

### Linux/Mac

#### Terminal 1 - BlobChangeFeedSync
```bash
nohup java -jar target/blob-util-1.0.0.jar my-config.conf > blob-sync.log 2>&1 &
```

#### Terminal 2 - BlobArchiveProcessor
```bash
nohup java -jar target/blob-util-archive-1.0.0.jar my-config.conf > blob-archive.log 2>&1 &
```

## Configuration

Both processes use the same configuration file (`my-config.conf`). Make sure it contains:

```hocon
blob {
  containerName = "test"              # Used by BlobChangeFeedSync
  archiveContainerName = "test"      # Used by BlobArchiveProcessor
  pollingIntervalSeconds = 60
  # ... other blob config ...
}

database {
  # ... database config ...
}
```

## Important Notes

1. **Both processes can run simultaneously** - They use different containers and don't interfere with each other
2. **Same database table** - Both write to the same table, but update different columns
3. **Independent polling** - Each process has its own polling interval
4. **No conflicts** - BlobChangeFeedSync writes blob metadata, BlobArchiveProcessor updates record counts

## Stopping the Processes

- **Windows**: Use Task Manager or `taskkill /F /IM java.exe` (will kill all Java processes)
- **Linux/Mac**: Find the process with `ps aux | grep java` and kill with `kill <PID>`
- **If running in foreground**: Press `Ctrl+C` in each terminal





