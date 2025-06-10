Here's the one-line PowerShell command:

```powershell
(Invoke-RestMethod -Uri "https://login.microsoftonline.com/YOUR-TENANT-ID/oauth2/v2.0/token" -Method Post -Body @{grant_type="client_credentials";client_id="YOUR-CLIENT-ID";client_secret="YOUR-CLIENT-SECRET";scope="https://graph.microsoft.com/.default"} -ContentType "application/x-www-form-urlencoded").access_token
```

Replace:
- `YOUR-TENANT-ID` with your Azure tenant ID
- `YOUR-CLIENT-ID` with your service principal client ID  
- `YOUR-CLIENT-SECRET` with your service principal secret

This will output just the bearer token string that you can use directly in API calls.

For different scopes, change the scope parameter:
- Azure Resource Manager: `"https://management.azure.com/.default"`
- Key Vault: `"https://vault.azure.net/.default"`