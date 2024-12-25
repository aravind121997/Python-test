trigger:
  - main

pool:
  vmImage: 'ubuntu-latest'

variables:
  - name: AZURE_KEY_VAULT_NAME
    value: 'your-key-vault-name'
  - name: GCP_PROJECT_ID
    value: 'your-gcp-project-id'
  - name: GCP_SECRET_NAME
    value: 'your-secret-name'
  - name: GCP_SA_KEY_SECRET_NAME
    value: 'gcp-service-account-key'  # Name of the secret in Azure Key Vault containing GCP credentials

steps:
- task: AzureKeyVault@2
  inputs:
    azureSubscription: 'your-azure-service-connection'
    KeyVaultName: '$(AZURE_KEY_VAULT_NAME)'
    SecretsFilter: '$(GCP_SA_KEY_SECRET_NAME)'
    RunAsPreJob: true

- bash: |
    # Create GCP service account key file from Azure KeyVault secret
    echo "$(GCP_SA_KEY_SECRET_NAME)" > gcp-key.json
    
    # Secure the key file
    chmod 600 gcp-key.json
  displayName: 'Setup GCP Credentials'

- task: GoogleCloudSDK@0
  inputs:
    version: 'latest'
    command: 'auth'
    serviceAccountKeyPath: 'gcp-key.json'
  displayName: 'Authenticate with GCP'

- bash: |
    # Install required Python packages
    pip install google-cloud-secret-manager azure-keyvault-secrets azure-identity
  displayName: 'Install Dependencies'

- task: AzureCLI@2
  inputs:
    azureSubscription: 'your-azure-service-connection'
    scriptType: 'bash'
    scriptLocation: 'inlineScript'
    inlineScript: |
      # Create Python script for secret sync
      cat << EOF > sync_secrets.py
      from google.cloud import secretmanager
      from azure.keyvault.secrets import SecretClient
      from azure.identity import DefaultAzureCredential
      import os
      import json
      from datetime import datetime

      def sync_secret():
          try:
              # GCP Secret Manager client setup
              gcp_client = secretmanager.SecretManagerServiceClient()
              secret_path = f"projects/${GCP_PROJECT_ID}/secrets/${GCP_SECRET_NAME}/versions/latest"

              # Get secret from GCP
              response = gcp_client.access_secret_version(request={"name": secret_path})
              secret_value = response.payload.data.decode("UTF-8")

              # Azure Key Vault client setup
              key_vault_url = f"https://${AZURE_KEY_VAULT_NAME}.vault.azure.net/"
              azure_client = SecretClient(vault_url=key_vault_url, credential=DefaultAzureCredential())

              # Add timestamp to secret tags
              tags = {
                  "synced_from": "gcp",
                  "sync_timestamp": datetime.utcnow().isoformat(),
                  "source_secret": "${GCP_SECRET_NAME}"
              }

              # Update secret in Azure Key Vault with tags
              azure_client.set_secret("${GCP_SECRET_NAME}", secret_value, tags=tags)
              print(f"✅ Secret '${GCP_SECRET_NAME}' successfully synced from GCP to Azure Key Vault")
              return True

          except Exception as e:
              print(f"❌ Error syncing secret: {str(e)}")
              raise

      if __name__ == "__main__":
          sync_secret()
      EOF

      # Run the sync script
      python sync_secrets.py
  displayName: 'Sync GCP Secret to Azure'

- bash: |
    # Cleanup sensitive files
    rm -f gcp-key.json
    rm -f sync_secrets.py
    rm -rf ~/.config/gcloud
  displayName: 'Cleanup'
  condition: always()# Python-test