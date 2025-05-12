import requests
import json
from azure.identity import DefaultAzureCredential

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
        print(f"Error getting auth header: {e}")
        raise e

def process_application_gateway_details():
    # Get authentication header
    auth_header = get_auth_header()

    # Azure Resource Graph API endpoint URL to get Application Gateway resources
    get_app_gw_resources_url = "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2022-10-01"

    # Query body to get Application Gateway resources
    get_app_gw_resources_body = {
        "query": "Resources | where type == 'microsoft.network/applicationgateways'"
    }

    # Convert body to JSON
    get_app_gw_resources_body_json = json.dumps(get_app_gw_resources_body)

    # Make API request to get App Gateway resources
    get_app_gw_resources_response = requests.post(
        url=get_app_gw_resources_url,
        headers=auth_header,
        data=get_app_gw_resources_body_json
    )

    # Convert response to JSON object
    get_app_gw_resources_object = json.loads(get_app_gw_resources_response.content)

    # Initialize list to store App Gateway details
    app_gw_details_list = []

    # Process each Application Gateway resource
    for app_gw_resource in get_app_gw_resources_object.get('data', []):
        # Extract ITSO and ITSO Delegate tags
        itso = app_gw_resource.get('tags', {}).get('ITSO', '')
        itso_delegate = app_gw_resource.get('tags', {}).get('ITSO Delegate', '')

        # Print processing information
        print(f"Processing AppGW Instance - {app_gw_resource.get('name', '')}")

        # Construct URL to get detailed App Gateway information
        get_app_gw_url = (
            f"https://management.azure.com/subscriptions/{app_gw_resource.get('subscriptionId', '')}"
            f"/resourceGroups/{app_gw_resource.get('resourceGroup', '')}"
            f"/providers/Microsoft.Network/applicationGateways/{app_gw_resource.get('name', '')}"
            f"?api-version=2024-05-01"
        )

        # Make API request to get App Gateway details
        get_app_gw_response = requests.get(
            url=get_app_gw_url,
            headers=auth_header
        )

        # Parse App Gateway response
        app_gw_details = get_app_gw_response.json()

        # Initialize variables for processing
        app_gw_endpoints = []
        waf_policy_name = ""
        split_waf_id = ""
        rate_limit_action = ""
        rate_limit_threshold = ""
        bot_ruleset_version = ""
        managed_ruleset_name = ""
        managed_ruleset_version = ""

        # Process HTTP Listeners
        for listener in app_gw_details.get('properties', {}).get('httpListeners', []):
            if listener.get('properties', {}).get('hostName'):
                app_gw_endpoints.append(listener['properties']['hostName'])

        # Process WAF Policy
        waf_policy_ref = app_gw_details.get('properties', {}).get('firewallPolicy', {}).get('id', '')
        if waf_policy_ref:
            split_waf_id = waf_policy_ref.split('/')

            # Get WAF Policy details URL
            get_app_gw_waf_url = (
                f"https://management.azure.com/subscriptions/{app_gw_resource.get('subscriptionId', '')}"
                f"/resourceGroups/{app_gw_resource.get('resourceGroup', '')}"
                f"/providers/Microsoft.Network/ApplicationGatewayWebApplicationFirewallPolicies/{waf_policy_ref.split('/')[-1]}"
                f"?api-version=2024-05-01"
            )

            # Make API request to get WAF Policy details
            get_app_gw_waf_response = requests.get(
                url=get_app_gw_waf_url,
                headers=auth_header
            )

            waf_policy_details = get_app_gw_waf_response.json()

            # Process custom rules
            for custom_rule in waf_policy_details.get('properties', {}).get('customRules', []):
                if custom_rule.get('ruleType') == 'RateLimitRule':
                    rate_limit_action = custom_rule.get('action', '')
                    rate_limit_threshold = custom_rule.get('rateLimitThreshold', '')

            # Process managed rule sets
            managed_rulesets = waf_policy_details.get('properties', {}).get('managedRules', {}).get('managedRuleSets', [])
            for ruleset in managed_rulesets:
                if ruleset.get('ruleSetType') == 'Microsoft.BotManagerRuleSet':
                    bot_ruleset_version = ruleset.get('ruleSetVersion', '')
                else:
                    managed_ruleset_name = ruleset.get('ruleSetType', '')
                    managed_ruleset_version = ruleset.get('ruleSetVersion', '')

        # Compile all details into a dictionary
        app_gw_details_entry = {
            'name': app_gw_resource.get('name', ''),
            'resourceGroup': app_gw_resource.get('resourceGroup', ''),
            'subscriptionId': app_gw_resource.get('subscriptionId', ''),
            'itso': itso,
            'itsoDelegate': itso_delegate,
            'endpoints': app_gw_endpoints,
            'wafPolicyName': waf_policy_name,
            'rateLimit': {
                'action': rate_limit_action,
                'threshold': rate_limit_threshold
            },
            'managedRuleset': {
                'name': managed_ruleset_name,
                'version': managed_ruleset_version
            },
            'botManagerRulesetVersion': bot_ruleset_version
        }

        app_gw_details_list.append(app_gw_details_entry)

    # Print summary of findings
    print(f"\nFound {len(app_gw_details_list)} Application Gateway profiles")
    for profile in app_gw_details_list:
        print(f"Profile: {profile['name']}")
        print(f"  Resource Group: {profile['resourceGroup']}")
        print(f"  Endpoints: {', '.join(profile['endpoints'])}")
        print(f"  WAF Rate Limit: {profile['rateLimit']}")
        print("  ----------------")

    return app_gw_details_list

# Run the script
if __name__ == "__main__":
    app_gw_details = process_application_gateway_details()