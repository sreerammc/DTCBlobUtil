# Blob Change Feed to PostgreSQL Sync Utility

A Java utility that synchronizes Azure Blob Storage change feed (insert and update events) to a PostgreSQL database along with metadata.

## Features

- Reads blob change events from Azure Blob Storage
- Filters for insert and update events only
- Stores blob metadata, properties, and change information in PostgreSQL
- Supports multiple authentication methods:
  - Connection string
  - Managed Identity
  - Service Principal (Client ID/Secret)
- Automatic table creation and schema management
- Resumable processing (tracks last processed timestamp)
- Efficient upsert operations to handle duplicates

## Prerequisites

- Java 11 or higher
- Maven 3.6+
- Azure Blob Storage account with appropriate permissions
- PostgreSQL database (9.5+)

## Setup

### 1. Clone and Build

```bash
mvn clean package
```

This will create a JAR file in `target/blob-util-1.0.0.jar`.

### 2. Configuration

Create an `application.conf` file in the root directory (or use environment variables):

```hocon
blob {
  connectionString = "DefaultEndpointsProtocol=https;AccountName=your_account;AccountKey=your_key;EndpointSuffix=core.windows.net"
  containerName = "your_container_name"
}

database {
  host = "localhost"
  port = 5432
  database = "your_database"
  username = "your_username"
  password = "your_password"
  schema = "public"
  tableName = "blob_changes"
  maxPoolSize = 10
}
```

### 3. Environment Variables (Alternative)

Instead of `application.conf`, you can use environment variables:

**Blob Storage:**
- `BLOB_CONNECTION_STRING` - Azure Blob Storage connection string
- `BLOB_ACCOUNT_NAME` - Storage account name (for managed identity/service principal)
- `BLOB_CONTAINER_NAME` - Container name
- `BLOB_USE_MANAGED_IDENTITY` - Set to "true" to use managed identity
- `BLOB_TENANT_ID` - Azure AD tenant ID (for service principal)
- `BLOB_CLIENT_ID` - Service principal client ID
- `BLOB_CLIENT_SECRET` - Service principal client secret

**PostgreSQL:**
- `DB_HOST` - Database host
- `DB_PORT` - Database port (default: 5432)
- `DB_DATABASE` - Database name
- `DB_USERNAME` - Database username
- `DB_PASSWORD` - Database password
- `DB_SCHEMA` - Schema name (default: "public")
- `DB_TABLE_NAME` - Table name (default: "blob_changes")
- `DB_MAX_POOL_SIZE` - Connection pool size (default: 10)

### 4. Authentication Methods

#### Connection String
```hocon
blob {
  connectionString = "DefaultEndpointsProtocol=https;AccountName=..."
  containerName = "your_container"
}
```

#### Managed Identity
```hocon
blob {
  useManagedIdentity = true
  accountName = "your_account_name"
  containerName = "your_container"
}
```

#### Service Principal
```hocon
blob {
  accountName = "your_account_name"
  tenantId = "your_tenant_id"
  clientId = "your_client_id"
  clientSecret = "your_client_secret"
  containerName = "your_container"
}
```

## Usage

### Run the Utility

**Basic usage (uses default `application.conf` or environment variables):**
```bash
java -jar target/blob-util-1.0.0.jar
```

**Specify a custom config file:**
```bash
java -jar target/blob-util-1.0.0.jar my-config.conf
```

**Using command-line option:**
```bash
java -jar target/blob-util-1.0.0.jar -c /path/to/config.conf
java -jar target/blob-util-1.0.0.jar --config application.conf
```

**Show help:**
```bash
java -jar target/blob-util-1.0.0.jar --help
```

**Or with Maven:**
```bash
mvn exec:java -Dexec.mainClass="com.dtc.blobutil.BlobChangeFeedSync" -Dexec.args="my-config.conf"
```

## Database Schema

The utility automatically creates the following table structure:

```sql
CREATE TABLE blob_changes (
    id BIGSERIAL PRIMARY KEY,
    blob_name VARCHAR(1024) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    content_type VARCHAR(255),
    content_length BIGINT,
    etag VARCHAR(255),
    last_modified TIMESTAMP WITH TIME ZONE,
    metadata JSONB,
    url TEXT,
    version_id VARCHAR(255),
    snapshot VARCHAR(255),
    previous_info TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(blob_name, event_type, last_modified)
);

CREATE INDEX idx_blob_changes_blob_name ON blob_changes(blob_name);
CREATE INDEX idx_blob_changes_last_modified ON blob_changes(last_modified);
```

## How It Works

1. **Initialization**: The utility loads configuration and initializes database connections
2. **Table Setup**: Creates the PostgreSQL table if it doesn't exist
3. **Resume Point**: Checks for the last processed timestamp to resume from where it left off
4. **Change Detection**: Scans the blob container for changes since the last processed timestamp
5. **Filtering**: Filters for insert and update events only (BlobCreated, BlobPropertiesUpdated, BlobMetadataUpdated)
6. **Upsert**: Inserts or updates records in PostgreSQL with blob metadata and properties

## Event Types

The utility processes the following event types:
- `BlobCreated` - New blob created
- `BlobPropertiesUpdated` - Blob properties changed
- `BlobMetadataUpdated` - Blob metadata changed

Other event types (BlobDeleted, BlobRenamed, etc.) are filtered out.

## Notes

- **Change Feed**: This implementation uses a polling-based approach to detect changes. For production use with Azure Blob Storage Change Feed enabled, you should use the Azure Change Feed SDK for more efficient event processing.
- **Performance**: For large containers, consider running the utility periodically or as a scheduled job.
- **Resumability**: The utility tracks the last processed timestamp, so you can safely re-run it to catch up on missed changes.

## Troubleshooting

### Connection Issues
- Verify your blob storage credentials and permissions
- Check database connectivity and credentials
- Ensure the container exists and is accessible

### Performance Issues
- Adjust `maxPoolSize` in database configuration
- Consider running during off-peak hours for large containers
- Monitor database connection pool usage

## License

This utility is provided as-is for internal use.


