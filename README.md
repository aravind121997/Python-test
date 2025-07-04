import requests
import json
from typing import Dict, List, Any, Optional

def get_frontdoor_cdn_details(client_id: str, client_secret: str, tenant_id: str, tenant_name: str) -> List[Dict[str, Any]]:
    """
    Retrieve Azure Front Door CDN details including WAF policies and security settings.
    
    Args:
        client_id (str): Azure service principal client ID
        client_secret (str): Azure service principal client secret
        tenant_id (str): Azure tenant ID
        tenant_name (str): Azure tenant name
        
    Returns:
        List[Dict[str, Any]]: List of Front Door CDN details with security information
    """
    
    def get_auth_token() -> str:
        """Get Azure authentication token"""
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'resource': 'https://management.azure.com/'
        }
        
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        return response.json()['access_token']
    
    def execute_resource_graph_query(query: str, token: str) -> Dict[str, Any]:
        """Execute Azure Resource Graph query"""
        url = "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2022-10-01"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        payload = {
            'query': query
        }
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    
    try:
        # Get authentication token
        auth_token = get_auth_token()
        
        # Query 1: Get CDN Resources
        cdn_query = """
        Resources 
        | where type == 'microsoft.cdn/profiles'
        """
        
        cdn_response = execute_resource_graph_query(cdn_query, auth_token)
        cdn_resources = cdn_response.get('data', [])
        
        frontdoor_object_array = []
        
        # Process each CDN resource
        for cdn_resource in cdn_resources:
            itso = cdn_resource.get('tags', {}).get('ITSO', '')
            
            if itso:
                itso_delegate = f"{itso} Delegate"
                eim = cdn_resource.get('tags', {}).get('EIM', '')
                production = cdn_resource.get('tags', {}).get('Production', '')
                
                print(f"Processing FD Instance - {cdn_resource.get('name', '')}")
                
                # Query 2: Get Front Door Endpoints
                subscription_id = cdn_resource.get('subscriptionId', '')
                resource_group = cdn_resource.get('resourceGroup', '')
                profile_name = cdn_resource.get('name', '')
                
                fd_endpoints_query = f"""
                Resources 
                | where subscriptionId == '{subscription_id}' 
                | where resourceGroup == '{resource_group}' 
                | where type == 'microsoft.cdn/profiles/afdendpoints'
                | where name contains '{profile_name}'
                """
                
                fd_endpoints_response = execute_resource_graph_query(fd_endpoints_query, auth_token)
                afd_endpoints = []
                
                for endpoint in fd_endpoints_response.get('data', []):
                    if endpoint.get('properties', {}).get('parameters', {}).get('wafPolicy', {}).get('id'):
                        afd_endpoints.append(endpoint.get('properties', {}).get('hostName', ''))
                
                afd_endpoints_str = ','.join(afd_endpoints)
                
                # Query 3: Get Custom Domains
                custom_domain_query = f"""
                Resources 
                | where subscriptionId == '{subscription_id}' 
                | where resourceGroup == '{resource_group}' 
                | where type == 'microsoft.cdn/profiles/customdomains'
                | where name contains '{profile_name}'
                """
                
                custom_domain_response = execute_resource_graph_query(custom_domain_query, auth_token)
                custom_domains = []
                
                for domain in custom_domain_response.get('data', []):
                    if domain.get('properties', {}).get('parameters', {}).get('wafPolicy', {}).get('id'):
                        custom_domains.append(domain.get('properties', {}).get('hostName', ''))
                
                custom_domains_str = ','.join(custom_domains)
                
                # Query 4: Get Security Policy
                security_policy_query = f"""
                Resources 
                | where subscriptionId == '{subscription_id}' 
                | where resourceGroup == '{resource_group}' 
                | where type == 'microsoft.cdn/profiles/securitypolicies'
                | where name contains '{profile_name}'
                """
                
                security_policy_response = execute_resource_graph_query(security_policy_query, auth_token)
                
                waf_policy_name = ""
                waf_policy_array = []
                sec_policy = ""
                split_waf_id = ""
                rate_limit_action = ""
                rate_limit_threshold = ""
                bot_ruleset_version = ""
                managed_ruleset_name = ""
                managed_ruleset_version = ""
                bad_bots_anti_bypass = ""
                
                for sec_policy_item in security_policy_response.get('data', []):
                    if sec_policy_item.get('properties', {}).get('parameters', {}).get('wafPolicy', {}).get('id'):
                        sec_policy = sec_policy_item.get('properties', {}).get('parameters', {}).get('wafPolicy', {}).get('id', '')
                        split_waf_id = sec_policy.split('/')
                        waf_policy_array.append(split_waf_id[8])  # Index 8 from the split
                
                waf_policy_name = ','.join(waf_policy_array)
                
                # Query 5: Get WAF Policy Details
                if waf_policy_name:
                    waf_policy_parts = split_waf_id
                    
                    # Build FDWAF URL
                    fdwaf_url = f"https://management.azure.com/subscriptions/{waf_policy_parts[2]}/resourceGroups/{waf_policy_parts[4]}/providers/Microsoft.Network/FrontDoorWebApplicationFirewallPolicies/{waf_policy_parts[8]}?api-version=2022-05-01"
                    
                    # Make direct API call for WAF policy details
                    headers = {
                        'Authorization': f'Bearer {auth_token}',
                        'Content-Type': 'application/json'
                    }
                    
                    try:
                        waf_response = requests.get(fdwaf_url, headers=headers)
                        waf_response.raise_for_status()
                        waf_data = waf_response.json()
                        
                        # Process WAF policy settings
                        policy_settings = waf_data.get('properties', {}).get('policySettings', {})
                        rate_limit_action = policy_settings.get('rateLimitAction', '')
                        rate_limit_threshold = policy_settings.get('rateLimitThreshold', '')
                        
                        # Process managed rule sets
                        managed_rules = waf_data.get('properties', {}).get('managedRules', {}).get('managedRuleSets', [])
                        
                        for managed_ruleset in managed_rules:
                            if managed_ruleset.get('ruleSetType') == 'Microsoft_BotManagerRuleSet':
                                bot_ruleset_version = managed_ruleset.get('ruleSetVersion', '')
                                bad_bots_anti_bypass = "pass"
                                
                                # Check for specific rule overrides
                                rule_overrides = managed_ruleset.get('ruleGroupOverrides', {}).get('rules', [])
                                for rule in rule_overrides:
                                    if rule.get('ruleId') in ['100100', '100200', '100300']:
                                        bad_bots_anti_bypass = "FAIL"
                                        break
                            else:
                                managed_ruleset_name = managed_ruleset.get('ruleSetType', '')
                                managed_ruleset_version = managed_ruleset.get('ruleSetVersion', '')
                    
                    except requests.exceptions.RequestException as e:
                        print(f"Error fetching WAF policy details: {e}")
                
                # Create the row value object
                row_value = {
                    'EIM': eim,
                    'Production': production,
                    'FrontDoorName': cdn_resource.get('name', ''),
                    'ResourceGroup': cdn_resource.get('resourceGroup', ''),
                    'SubscriptionId': cdn_resource.get('subscriptionId', ''),
                    'ITSO': itso,
                    'ITSODelegate': itso_delegate,
                    'EndpointUrl': afd_endpoints_str,
                    'CustomDomain': custom_domains_str,
                    'WAFPolicyName': waf_policy_name,
                    'WAFMode': '',  # Not available in Resource Graph query
                    'WAFStatus': '',  # Not available in Resource Graph query
                    'RateLimitAction': rate_limit_action,
                    'RateLimitThreshold': rate_limit_threshold,
                    'BadBotsAntiBypass': bad_bots_anti_bypass,
                    'BotManagerRulesetVersion': bot_ruleset_version,
                    'ManagedRulesetName': managed_ruleset_name,
                    'ManagedRulesetVersion': managed_ruleset_version
                }
                
                frontdoor_object_array.append(row_value)
        
        return frontdoor_object_array
    
    except requests.exceptions.RequestException as e:
        print(f"Error executing Azure API request: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

# Example usage
if __name__ == "__main__":
    # Example function call
    try:
        results = get_frontdoor_cdn_details(
            client_id="your-client-id",
            client_secret="your-client-secret", 
            tenant_id="your-tenant-id",
            tenant_name="your-tenant-name"
        )
        
        # Export to CSV or process as needed
        import pandas as pd
        df = pd.DataFrame(results)
        df.to_csv('FrontDoor.csv', index=False)
        print(f"Exported {len(results)} Front Door configurations to FrontDoor.csv")
        
    except Exception as e:
        print(f"Error: {e}")