import requests
import json
import csv
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_access_token(client_id, client_secret, tenant_id, tenant_name=None):
    """
    Extract access token using service principal authentication
    
    Args:
        client_id (str): Azure AD application (client) ID
        client_secret (str): Azure AD application client secret
        tenant_id (str): Azure AD tenant ID
        tenant_name (str): Azure AD tenant name (optional)
    
    Returns:
        str: Access token for Azure REST API calls
    """
    try:
        # Azure AD OAuth2 token endpoint
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        
        # Request payload for client credentials flow
        payload = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'https://management.azure.com/.default'
        }
        
        # Request headers
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        logger.info(f"Requesting access token for tenant: {tenant_name or tenant_id}")
        
        # Make the token request
        response = requests.post(token_url, data=payload, headers=headers)
        response.raise_for_status()
        
        token_data = response.json()
        access_token = token_data.get('access_token')
        
        if not access_token:
            raise ValueError("No access token received in response")
            
        logger.info("Access token retrieved successfully")
        return access_token
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error requesting access token: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during token extraction: {str(e)}")
        raise

def apl_gateway_details(client_id, client_secret, tenant_id, tenant_name, subscription_id):
    """
    Extract Azure Application Gateway details
    
    Args:
        client_id (str): Azure AD application (client) ID
        client_secret (str): Azure AD application client secret
        tenant_id (str): Azure AD tenant ID
        tenant_name (str): Azure AD tenant name
        subscription_id (str): Azure subscription ID
    
    Returns:
        list: List of application gateway details
    """
    try:
        # Get access token
        access_token = get_access_token(client_id, client_secret, tenant_id, tenant_name)
        
        # Set up headers for Azure REST API calls
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        # Get Application Gateway resources
        appgw_url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Network/applicationGateways?api-version=2022-10-01"
        
        logger.info("Fetching Application Gateway resources...")
        appgw_response = requests.get(appgw_url, headers=headers)
        appgw_response.raise_for_status()
        
        appgw_data = appgw_response.json()
        app_gw_resources = appgw_data.get('value', [])
        
        logger.info(f"Found {len(app_gw_resources)} Application Gateway resources")
        
        app_gw_object_array = []
        
        for app_gw_resource in app_gw_resources:
            logger.info(f"Processing Application Gateway: {app_gw_resource.get('name', 'Unknown')}")
            
            # Initialize variables with empty strings
            waf_policy_name = ""
            get_app_gw_url = ""
            get_app_gw_response = ""
            get_app_gw_maf_url = ""
            get_app_gw_maf_response = ""
            msg_name = ""
            eim = ""
            production = ""
            rate_limit_action = ""
            rate_limit_threshold = ""
            fdid_anti_bypass = ""
            bad_bots_anti_bypass = ""
            bot_ruleset_version = ""
            managed_ruleset_name = ""
            managed_ruleset_version = ""
            split_waf_id = ""
            app_gw_endpoints = ""
            
            # Process each resource
            for key, value in app_gw_resource.items():
                if key == 'wafPolicyName':
                    waf_policy_name = value or ""
                elif key == 'getAppGWUrl':
                    get_app_gw_url = value or ""
                elif key == 'getAppGWResponse':
                    get_app_gw_response = value or ""
                elif key == 'getAppGWMAFUrl':
                    get_app_gw_maf_url = value or ""
                elif key == 'getAppGWMAFResponse':
                    get_app_gw_maf_response = value or ""
                elif key == 'msgName':
                    msg_name = value or ""
                elif key == 'eim':
                    eim = value or ""
                elif key == 'production':
                    production = value or ""
                elif key == 'rateLimitAction':
                    rate_limit_action = value or ""
                elif key == 'rateLimitThreshold':
                    rate_limit_threshold = value or ""
                elif key == 'fdidAntiBypass':
                    fdid_anti_bypass = value or ""
                elif key == 'badBotsAntiBypass':
                    bad_bots_anti_bypass = value or ""
                elif key == 'botRulesetVersion':
                    bot_ruleset_version = value or ""
                elif key == 'managedRulesetName':
                    managed_ruleset_name = value or ""
                elif key == 'managedRulesetVersion':
                    managed_ruleset_version = value or ""
                elif key == 'splitWafId':
                    split_waf_id = value or ""
            
            # Get Application Gateway details
            resource_name = app_gw_resource.get('name', '')
            resource_group = app_gw_resource.get('resourceGroup', '')
            subscription_id_from_resource = app_gw_resource.get('subscriptionId', subscription_id)
            
            get_app_gw_url = f"https://management.azure.com/subscriptions/{subscription_id_from_resource}/resourceGroups/{resource_group}/providers/Microsoft.Network/applicationGateways/{resource_name}?api-version=2024-05-01"
            
            logger.info(f"Fetching details for Application Gateway: {resource_name}")
            get_app_gw_response = requests.get(get_app_gw_url, headers=headers)
            get_app_gw_response.raise_for_status()
            
            get_app_gw_data = get_app_gw_response.json()
            
            # Extract subnet information
            subnet_id = ""
            subnet_url = ""
            
            gateway_ip_configs = get_app_gw_data.get('properties', {}).get('gatewayIPConfigurations', [])
            if gateway_ip_configs:
                subnet_id = gateway_ip_configs[0].get('properties', {}).get('subnet', {}).get('id', '')
                if subnet_id:
                    subnet_url = f"https://management.azure.com{subnet_id}?api-version=2023-04-01"
            
            # Extract network security group name
            nsg_name = ""
            if subnet_url:
                subnet_response = requests.get(subnet_url, headers=headers)
                if subnet_response.status_code == 200:
                    subnet_data = subnet_response.json()
                    nsg_id = subnet_data.get('properties', {}).get('networkSecurityGroup', {}).get('id', '')
                    if nsg_id:
                        nsg_name_parts = nsg_id.split('/')
                        if nsg_name_parts:
                            nsg_name = nsg_name_parts[-1].strip("'")
            
            # Extract public WAF information
            public_waf = get_app_gw_data.get('tags', {}).get('PublicWAF', '')
            
            # Process HTTP listeners for endpoints
            app_gw_endpoint_array = []
            http_listeners = get_app_gw_data.get('properties', {}).get('httpListeners', [])
            
            for listener in http_listeners:
                host_name = listener.get('properties', {}).get('hostName', '')
                if host_name:
                    app_gw_endpoint_array.append(host_name)
            
            app_gw_endpoints = ",".join(app_gw_endpoint_array)
            
            # Extract firewall policy information
            firewall_policy = get_app_gw_data.get('properties', {}).get('firewallPolicy', {})
            if firewall_policy:
                firewall_policy_id = firewall_policy.get('id', '')
                split_waf_id = firewall_policy_id.split('/')
                if len(split_waf_id) > 8:
                    waf_policy_name = split_waf_id[8]
                    
                    # Get WAF policy details
                    subscription_id_waf = split_waf_id[2]
                    resource_group_waf = split_waf_id[4]
                    
                    get_app_gw_maf_url = f"https://management.azure.com/subscriptions/{subscription_id_waf}/resourceGroups/{resource_group_waf}/providers/Microsoft.Network/ApplicationGatewayWebApplicationFirewallPolicies/{waf_policy_name}?api-version=2024-05-01"
                    
                    get_app_gw_maf_response = requests.get(get_app_gw_maf_url, headers=headers)
                    if get_app_gw_maf_response.status_code == 200:
                        get_app_gw_maf_data = get_app_gw_maf_response.json()
                        
                        # Process custom rules
                        custom_rules = get_app_gw_maf_data.get('properties', {}).get('customRules', [])
                        for custom_rule in custom_rules:
                            rule_type = custom_rule.get('ruleType', '')
                            if rule_type == 'RateLimitRule':
                                rate_limit_action = custom_rule.get('action', '')
                                rate_limit_threshold = custom_rule.get('rateLimitThreshold', '')
                        
                        # Determine FDID Anti Bypass
                        if public_waf == "Yes":
                            fdid_custom_rules = [rule for rule in custom_rules if rule.get('name') == 'AllowFDID']
                            if fdid_custom_rules:
                                fdid_rule = fdid_custom_rules[0]
                                if (fdid_rule.get('action') == 'Allow' and 
                                    fdid_rule.get('state') == 'Enabled'):
                                    fdid_anti_bypass = "pass"
                                else:
                                    fdid_anti_bypass = "FAIL"
                            else:
                                fdid_anti_bypass = "FAIL"
                        else:
                            fdid_anti_bypass = "no"
                        
                        # Process managed rules
                        managed_rules = get_app_gw_maf_data.get('properties', {}).get('managedRules', {})
                        managed_rule_sets = managed_rules.get('managedRuleSets', [])
                        
                        for managed_ruleset in managed_rule_sets:
                            ruleset_type = managed_ruleset.get('ruleSetType', '')
                            if ruleset_type == 'Microsoft_BotManagerRuleSet':
                                bot_ruleset_version = managed_ruleset.get('ruleSetVersion', '')
                                bad_bots_anti_bypass = "pass"
                                
                                # Check for specific rule overrides
                                rule_group_overrides = managed_ruleset.get('ruleGroupOverrides', {})
                                rules = rule_group_overrides.get('rules', [])
                                
                                for rule in rules:
                                    rule_id = rule.get('ruleId', '')
                                    if rule_id in [100100, 100200, 100300]:
                                        bad_bots_anti_bypass = "FAIL"
                                        break
                            else:
                                managed_ruleset_name = managed_ruleset.get('ruleSetType', '')
                                managed_ruleset_version = managed_ruleset.get('ruleSetVersion', '')
            
            # Create row values
            row_value = {
                'EIM': eim,
                'Production': production,
                'AppGwName': app_gw_resource.get('name', ''),
                'ResourceGroup': app_gw_resource.get('resourceGroup', ''),
                'SubscriptionId': app_gw_resource.get('subscriptionId', ''),
                'ITSO': '',  # This would need to be populated based on your logic
                'ITSODelegate': '',  # This would need to be populated based on your logic
                'EndpointUrl': app_gw_endpoints,
                'NSGname': nsg_name,
                'WAFPolicyName': waf_policy_name,
                'WAFMode': get_app_gw_maf_data.get('properties', {}).get('policySettings', {}).get('mode', '') if 'get_app_gw_maf_data' in locals() else '',
                'WAFStatus': get_app_gw_maf_data.get('properties', {}).get('policySettings', {}).get('state', '') if 'get_app_gw_maf_data' in locals() else '',
                'RateLimitAction': rate_limit_action,
                'RateLimitThreshold': rate_limit_threshold,
                'FDIdAntiBypass': fdid_anti_bypass,
                'BadBotsAntiBypass': bad_bots_anti_bypass,
                'BotManagerRulesetVersion': bot_ruleset_version,
                'ManagedRulesetName': managed_ruleset_name,
                'ManagedRulesetVersion': managed_ruleset_version
            }
            
            app_gw_object_array.append(row_value)
        
        return app_gw_object_array
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during API request: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during gateway details extraction: {str(e)}")
        raise

def export_to_csv(data, filename="AppGateways.csv"):
    """
    Export application gateway data to CSV file
    
    Args:
        data (list): List of application gateway details
        filename (str): Output CSV filename
    """
    try:
        if not data:
            logger.warning("No data to export")
            return
        
        fieldnames = [
            'EIM', 'Production', 'AppGwName', 'ResourceGroup', 'SubscriptionId',
            'ITSO', 'ITSODelegate', 'EndpointUrl', 'NSGname', 'WAFPolicyName',
            'WAFMode', 'WAFStatus', 'RateLimitAction', 'RateLimitThreshold',
            'FDIdAntiBypass', 'BadBotsAntiBypass', 'BotManagerRulesetVersion',
            'ManagedRulesetName', 'ManagedRulesetVersion'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        
        logger.info(f"Data exported successfully to {filename}")
        
    except Exception as e:
        logger.error(f"Error exporting to CSV: {str(e)}")
        raise

def main():
    """
    Main function to execute the Application Gateway details extraction
    """
    # Configuration - Replace with your actual values
    CLIENT_ID = "your-client-id"
    CLIENT_SECRET = "your-client-secret"
    TENANT_ID = "your-tenant-id"
    TENANT_NAME = "your-tenant-name"
    SUBSCRIPTION_ID = "your-subscription-id"
    
    try:
        logger.info("Starting Application Gateway details extraction...")
        
        # Extract Application Gateway details
        app_gateway_data = apl_gateway_details(
            CLIENT_ID, 
            CLIENT_SECRET, 
            TENANT_ID, 
            TENANT_NAME, 
            SUBSCRIPTION_ID
        )
        
        # Export to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"AppGateways_{timestamp}.csv"
        export_to_csv(app_gateway_data, output_filename)
        
        logger.info(f"Process completed successfully. Found {len(app_gateway_data)} Application Gateways")
        
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()