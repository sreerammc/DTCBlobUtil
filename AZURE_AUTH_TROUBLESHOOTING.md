# Azure Authentication Troubleshooting

## Common Errors

### Error: Application with identifier not found
The `clientId` format may be incorrect. Azure AD Application (Client) IDs should be in GUID format like:
- ✅ Correct: `"12345678-1234-1234-1234-123456789abc"`
- ❌ Incorrect: `"abc123~DEF456-ghi789"` (this looks like a secret value, not a client ID)

## Solutions

### Option 1: Use Connection String (Easiest)
If you have the Azure Storage connection string, use it instead:

1. Get your connection string from Azure Portal:
   - Go to your Storage Account → Access Keys
   - Copy the "Connection string"

2. Update `my-config.conf`:
```hocon
blob {
  connectionString = "DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT_NAME;AccountKey=YOUR_ACCOUNT_KEY;EndpointSuffix=core.windows.net"
  containerName = "your_container_name"
}
```

### Option 2: Fix Service Principal Credentials
If you need to use service principal:

1. **Verify Client ID (Application ID)**:
   - Go to Azure Portal → Azure Active Directory → App registrations
   - Find your application
   - Copy the "Application (client) ID" - it should be a GUID format

2. **Verify Tenant ID**:
   - Azure Portal → Azure Active Directory → Overview
   - Copy the "Tenant ID"

3. **Get Client Secret** (IMPORTANT - Common Mistake!):
   - Azure Portal → App registrations → Your app → Certificates & secrets
   - **DO NOT use the Secret ID** (GUID format like `12345678-1234-1234-1234-123456789abc`)
   - You need the **Secret Value** which looks like: `abc123~DEF456-ghi789-JKL012-mno345`
   - If you don't have the value (it's hidden after creation), create a new client secret:
     - Click "+ New client secret"
     - Add description and expiration
     - Click "Add"
     - **IMMEDIATELY copy the Value** (shown only once!)
   - The Value is NOT a GUID - it's a longer string with special characters

4. **Update `my-config.conf`**:
```hocon
blob {
  accountName = "your_account_name"
  tenantId = "YOUR_TENANT_ID_GUID"
  clientId = "YOUR_CLIENT_ID_GUID"  # Should be GUID format!
  clientSecret = "YOUR_CLIENT_SECRET"
  containerName = "your_container_name"
}
```

### Option 3: Use Managed Identity
If running on Azure (VM, App Service, etc.):

```hocon
blob {
  useManagedIdentity = true
  accountName = "your_account_name"
  containerName = "your_container_name"
}
```

## Required Permissions
Make sure your service principal or managed identity has:
- **Storage Blob Data Contributor** role on the storage account, OR
- **Storage Blob Data Reader** role (if read-only access is sufficient)

## Verify Credentials Format
- **Client ID**: GUID format (e.g., `12345678-1234-1234-1234-123456789abc`)
- **Tenant ID**: GUID format (e.g., `12345678-1234-1234-1234-123456789abc`)
- **Client Secret**: String value (can contain special characters)
- **Account Name**: Storage account name (lowercase, no special characters)

