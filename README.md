import requests
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient

def get_auth_header():
    """
    Obtain authentication header for Azure API calls
    """
    try:
        # Use DefaultAzureCredential for authentication
        credential = DefaultAzureCredential()
        
        # Get access token
        token = credential.get_token('https://management.azure.com/.default')
        
        # Create authorization header
        auth_header = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token.token}'
        }
        
        return auth_header
    except Exception as e:
        print(f"Error getting auth header: {e}")
        raise

def get_front_door_resources():
    """
    Retrieve and process Front Door resources
    """
    try:
        # Get authentication header
        auth_header = get_auth_header()
        
        # Construct the URL for retrieving resources
        subscription_id = '<your-subscription-id>'  # Replace with your subscription ID
        resource_group = '<your-resource-group>'  # Replace with your resource group
        
        get_fdr_url = (
            f'https://management.azure.com/subscriptions/{subscription_id}/'
            f'resourceGroups/{resource_group}/providers/Microsoft.Network/'
            f'frontDoors?api-version=2022-10-01'
        )
        
        # Make the API request
        response = requests.get(get_fdr_url, headers=auth_header)
        response.raise_for_status()
        
        # Parse the response
        front_door_resources = response.json()
        front_door_object_array = []
        
        # Process each Front Door resource
        for fd_resource in front_door_resources.get('value', []):
            row_value = {
                'FrontDoorName': fd_resource.get('name', ''),
                'ResourceGroup': fd_resource.get('resourceGroup', ''),
                'SubscriptionId': subscription_id,
                'ITSO': fd_resource.get('tags', {}).get('ITSO', ''),
                'SitsoDelegate': fd_resource.get('tags', {}).get('SitsoDelegate', ''),
                'EndpointUrl': '',  # You may need to extract this based on your specific resource structure
                'WAFPolicyName': '',
                'WAFMode': '',
                'RateLimitAction': '',
                'RateLimitThreshold': '',
                'ManagedRuleSetVersion': '',
                'ManagedRuleSetName': ''
            }
            
            # Process WAF policies and managed rule sets
            waf_policy_link = fd_resource.get('properties', {}).get('webApplicationFirewallPolicyLink', {})
            if waf_policy_link:
                waf_policy_url = waf_policy_link.get('id', '')
                if waf_policy_url:
                    # You would make another API call here to get WAF policy details
                    # This is a placeholder for that logic
                    waf_policy_response = requests.get(waf_policy_url, headers=auth_header)
                    waf_policy_data = waf_policy_response.json()
                    
                    # Extract WAF policy details
                    row_value['WAFPolicyName'] = waf_policy_data.get('name', '')
                    row_value['WAFMode'] = waf_policy_data.get('properties', {}).get('policySettings', {}).get('mode', '')
                    
                    # Process custom rules and managed rule sets
                    custom_rules = waf_policy_data.get('properties', {}).get('customRules', {}).get('rules', [])
                    managed_rule_sets = waf_policy_data.get('properties', {}).get('managedRules', {}).get('managedRuleSets', [])
                    
                    for custom_rule in custom_rules:
                        if custom_rule.get('ruleType') == 'RateLimitRule':
                            row_value['RateLimitAction'] = custom_rule.get('action', '')
                            row_value['RateLimitThreshold'] = custom_rule.get('rateLimitThreshold', '')
                    
                    for managed_rule_set in managed_rule_sets:
                        if managed_rule_set.get('ruleSetType') == 'Microsoft.BotManagerRuleSet':
                            row_value['ManagedRuleSetVersion'] = managed_rule_set.get('ruleSetVersion', '')
                        else:
                            row_value['ManagedRuleSetName'] = managed_rule_set.get('ruleSetType', '')
                            row_value['ManagedRuleSetVersion'] = managed_rule_set.get('ruleSetVersion', '')
            
            front_door_object_array.append(row_value)
        
        return front_door_object_array
    
    except Exception as e:
        print(f"Error processing Front Door resources: {e}")
        raise

def main():
    try:
        # Retrieve and process Front Door resources
        front_door_resources = get_front_door_resources()
        
        # Print or further process the resources
        for resource in front_door_resources:
            print(resource)
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    main()