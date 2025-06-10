import json
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import os

# Import requests inside functions to avoid circular import issues
try:
    import requests
except ImportError:
    requests = None

def get_azure_bearer_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    scope: str = "https://graph.microsoft.com/.default"
) -> Dict[str, Any]:
    """
    Fetch bearer token from Azure Service Principal using client credentials flow.
    
    Args:
        tenant_id (str): Azure AD tenant ID
        client_id (str): Service Principal application (client) ID
        client_secret (str): Service Principal client secret
        scope (str): The scope for the token. Defaults to Microsoft Graph API.
                    Other common scopes:
                    - "https://management.azure.com/.default" (Azure Resource Manager)
                    - "https://vault.azure.net/.default" (Key Vault)
    
    Returns:
        Dict containing:
        - access_token: The bearer token
        - token_type: Usually "Bearer"
        - expires_in: Token lifetime in seconds
        - expires_at: Calculated expiration timestamp
    
    Raises:
        requests.exceptions.RequestException: If the HTTP request fails
        ValueError: If the response doesn't contain expected token data
    """
    
    # Import requests here to avoid circular import
    import requests
    
    # Azure AD token endpoint
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    # Request headers
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    # Request body for client credentials flow
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope
    }
    
    try:
        # Make the token request
        response = requests.post(token_url, headers=headers, data=data)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        # Parse the response
        token_data = response.json()
        
        # Validate response contains required fields
        if "access_token" not in token_data:
            raise ValueError("Response does not contain access_token")
        
        # Calculate expiration time
        expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
        expires_at = datetime.now() + timedelta(seconds=expires_in)
        
        return {
            "access_token": token_data["access_token"],
            "token_type": token_data.get("token_type", "Bearer"),
            "expires_in": expires_in,
            "expires_at": expires_at,
            "scope": token_data.get("scope", scope)
        }
        
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(f"Failed to fetch token: {str(e)}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON response: {str(e)}")
    except Exception as e:
        raise ValueError(f"Unexpected error: {str(e)}")

def get_azure_bearer_token_from_env(
    scope: str = "https://graph.microsoft.com/.default"
) -> Dict[str, Any]:
    """
    Convenience function to fetch bearer token using environment variables.
    
    Expected environment variables:
    - AZURE_TENANT_ID
    - AZURE_CLIENT_ID  
    - AZURE_CLIENT_SECRET
    
    Args:
        scope (str): The scope for the token
        
    Returns:
        Dict containing token information
        
    Raises:
        ValueError: If required environment variables are missing
    """
    
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret]):
        missing_vars = []
        if not tenant_id: missing_vars.append("AZURE_TENANT_ID")
        if not client_id: missing_vars.append("AZURE_CLIENT_ID")
        if not client_secret: missing_vars.append("AZURE_CLIENT_SECRET")
        
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    return get_azure_bearer_token(tenant_id, client_id, client_secret, scope)

# Example usage
if __name__ == "__main__":
    try:
        # Method 1: Direct parameters
        token_info = get_azure_bearer_token(
            tenant_id="your-tenant-id",
            client_id="your-client-id", 
            client_secret="your-client-secret",
            scope="https://graph.microsoft.com/.default"
        )
        
        # Method 2: From environment variables
        # token_info = get_azure_bearer_token_from_env()
        
        print(f"Token obtained successfully!")
        print(f"Token type: {token_info['token_type']}")
        print(f"Expires at: {token_info['expires_at']}")
        print(f"Access token: {token_info['access_token'][:50]}...")
        
        # Use the token in API calls
        headers = {
            "Authorization": f"{token_info['token_type']} {token_info['access_token']}",
            "Content-Type": "application/json"
        }
        
    except Exception as e:
        print(f"Error: {e}")