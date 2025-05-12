import requests
import json
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient

def get_auth_header():
    """
    Create authentication header for Azure REST API calls
    """
    try:
        # Get Azure credentials and token
        credential = DefaultAzureCredential()
        # Get access token for Azure Resource Manager
        token = credential.get_token("https://management.azure.com/.default")
        
        # Create authorization header
        auth_header = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token.token}"
        }
        return auth_header
    except Exception as e:
        print(f"Error getting auth header: Error: {e}")
        raise e

# Get authentication header
auth_header = get_auth_header()

# Azure CDN/Front Door Resource Graph API endpoint URL
get_cdn_resources_url = "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2022-10-01"

# Query body to get CDN/Front Door profiles
get_cdn_resources_body = {
    "query": "Resources | where type == 'microsoft.cdn/profiles'"
}

# Convert body to JSON
get_cdn_resources_body_json = json.dumps(get_cdn_resources_body)

# Make API request to get CDN resources
get_cdn_resources_response = requests.post(
    url=get_cdn_resources_url,
    headers=auth_header,
    data=get_cdn_resources_body_json
)

# Convert response to JSON object
get_cdn_resources_object = json.loads(get_cdn_resources_response.content)

# Initialize array for Front Door objects
front_door_object_array = []

# Process each CDN resource
for cdn_resource in get_cdn_resources_object.get('data', []):
    # Extract ITSO and ITSO Delegate tags
    itso = cdn_resource.get('tags', {}).get('ITSO', '')
    itso_delegate = cdn_resource.get('tags', {}).get('ITSO-Delegate', '')
    
    # Log processing information
    print(f"Processing FD Instance - {cdn_resource.get('name', '')}")
    
    # Create row value with resource properties
    row_value = ""
    # Note: In Python, we'd typically build this as a dict or list, 
    # but keeping similar to original script structure
    row_value = " | Select FrontDoorName, ResourceGroup, SubscriptionId, ITSO, ITSODelegate, EndpointUrl, CustomDomain, WAFPolicyName, WAFMode, WAFStatus, RateLimitAction, RateLimitThreshold, BotManagerRulesetVersion, ManagedRulesetName, ManagedRulesetVersion"
    row_value += f"\n, {cdn_resource.get('id', '')}"
    
    # Get Front Door endpoints URL
    get_fd_endpoints_url = f"https://management.azure.com/subscriptions/{cdn_resource.get('subscriptionId', '')}/resourceGroups/{cdn_resource.get('resourceGroup', '')}/providers/Microsoft.Cdn/profiles/{cdn_resource.get('name', '')}/endpoints?api-version=2023-05-01"
    
    # Make API request to get endpoints
    get_fd_endpoints_response = requests.get(
        url=get_fd_endpoints_url,
        headers=auth_header
    )
    
    # Initialize endpoints variables
    afd_endpoints = ""
    afd_endpoint_array = []
    
    # Process each endpoint
    for endpoint in get_fd_endpoints_response.json().get('value', {}).get('properties', []):
        # Commented out line about SecPolicy properties
        # $SecPolicy.properties.parameters.wafPolicy.id
        afd_endpoint_array.append(endpoint.get('hostName', ''))
    
    # Join endpoints with comma
    afd_endpoints = ",".join(afd_endpoint_array)
    
    # Get custom domains URL
    get_fd_custom_domain_url = f"https://management.azure.com/subscriptions/{cdn_resource.get('subscriptionId', '')}/resourceGroups/{cdn_resource.get('resourceGroup', '')}/providers/Microsoft.Cdn/profiles/{cdn_resource.get('name', '')}/customDomains?api-version=2023-05-01"
    
    # Make API request to get custom domains
    get_fd_custom_domain_response = requests.get(
        url=get_fd_custom_domain_url,
        headers=auth_header
    )
    
    # Initialize custom domains variables
    custom_domains = ""
    custom_domain_array = []
    
    # Process each custom domain
    for domain in get_fd_custom_domain_response.json().get('value', {}).get('properties', []):
        # Commented out line about SecPolicy properties
        # $SecPolicy.properties.parameters.wafPolicy.id
        custom_domain_array.append(domain.get('hostName', ''))
    
    # Join custom domains with comma
    custom_domains = ",".join(custom_domain_array)
    
    # Get security policy URL
    get_fd_security_policy_url = f"https://management.azure.com/subscriptions/{cdn_resource.get('subscriptionId', '')}/resourceGroups/{cdn_resource.get('resourceGroup', '')}/providers/Microsoft.Cdn/profiles/{cdn_resource.get('name', '')}/securityPolicies?api-version=2023-05-01"
    
    # Make API request to get security policies
    get_fd_security_policy_response = requests.get(
        url=get_fd_security_policy_url,
        headers=auth_header
    )
    
    # Initialize security policy variables
    waf_policy_name = ""
    waf_policy_array = []
    sec_policy = ""
    get_fd_waf_url = ""
    split_waf_id = ""
    rate_limit_action = ""
    rate_limit_threshold = ""
    bot_ruleset_version = ""
    managed_ruleset_name = ""
    managed_ruleset_version = ""
    get_fd_waf_response = ""
    
    # Additional processing would continue here for WAF policies
    
    # Add processed data to front_door_object_array
    front_door_object_array.append({
        "name": cdn_resource.get('name', ''),
        "resourceGroup": cdn_resource.get('resourceGroup', ''),
        "subscriptionId": cdn_resource.get('subscriptionId', ''),
        "itso": itso,
        "itsoDelegate": itso_delegate,
        "endpoints": afd_endpoints,
        "customDomains": custom_domains,
        "wafPolicyName": waf_policy_name,
        # Add other properties as needed
    })

# Output or further process the front_door_object_array
print(f"Found {len(front_door_object_array)} Front Door profiles")
for profile in front_door_object_array:
    print(f"Profile: {profile['name']}")
    print(f"  Resource Group: {profile['resourceGroup']}")
    print(f"  Endpoints: {profile['endpoints']}")
    print(f"  Custom Domains: {profile['customDomains']}")
    print("  ----------------")