import requests
import json
import pandas as pd

def fetch_azure_frontdoor_waf_details(client_id, client_secret, tenant_id, subscription_id, tenant_name):
    # Get Auth Token from Azure AD
    def get_auth_token():
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        payload = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'https://management.azure.com/.default'
        }
        headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        return response.json()['access_token']

    token = get_auth_token()
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Query Resource Graph
    query = {
        "query": "Resources | where type == 'microsoft.network/frontdoors'"
    }

    graph_url = "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2022-10-01"
    response = requests.post(graph_url, headers=headers, json=query)
    response.raise_for_status()
    data = response.json()['data']

    # Prepare final output
    output_data = []

    for fd in data:
        fd_info = {
            "EIM": fd['tags'].get('EIM', ''),
            "Production": fd['tags'].get('Production', ''),
            "FrontDoorName": fd['name'],
            "ResourceGroup": fd['resourceGroup'],
            "SubscriptionId": fd['subscriptionId'],
            "ITSO": fd['tags'].get('ITSO', ''),
            "ITSODelegate": fd['tags'].get('ITSO Delegate', ''),
            "EndpointUrl": '',
            "WAFPolicyName": '',
            "WAFMode": '',
            "WAFStatus": '',
            "RateLimitAction": '',
            "RateLimitThreshold": '',
            "BadBotsAntiBypass": '',
            "BotManagerRulesetVersion": '',
            "ManagedRulesetName": '',
            "ManagedRulesetVersion": ''
        }

        # Get Frontend endpoints
        fd_detail_url = f"https://management.azure.com/subscriptions/{fd['subscriptionId']}/resourceGroups/{fd['resourceGroup']}/providers/Microsoft.Network/frontdoors/{fd['name']}?api-version=2019-05-01"
        fd_response = requests.get(fd_detail_url, headers=headers).json()

        endpoints = fd_response.get("properties", {}).get("frontendEndpoints", [])
        for endpoint in endpoints:
            fd_info["EndpointUrl"] = endpoint.get("properties", {}).get("hostName", '')
            waf_policy_link = endpoint.get("properties", {}).get("webApplicationFirewallPolicyLink", {}).get("id", '')
            waf_parts = waf_policy_link.split('/')
            if len(waf_parts) >= 9:
                waf_policy_name = waf_parts[8]
                fd_info["WAFPolicyName"] = waf_policy_name

                waf_url = f"https://management.azure.com/subscriptions/{waf_parts[2]}/resourceGroups/{waf_parts[4]}/providers/Microsoft.Network/frontdoorWebApplicationFirewallPolicies/{waf_policy_name}?api-version=2022-05-01"
                waf_response = requests.get(waf_url, headers=headers).json()
                properties = waf_response.get("properties", {})
                fd_info["WAFMode"] = properties.get("policySettings", {}).get("mode", '')
                fd_info["WAFStatus"] = properties.get("policySettings", {}).get("enabledState", '')

                # Custom rules
                for rule in properties.get("customRules", {}).get("rules", []):
                    if rule.get("ruleType") == "RateLimitRule":
                        fd_info["RateLimitAction"] = rule.get("action", '')
                        fd_info["RateLimitThreshold"] = rule.get("rateLimitThreshold", '')

                # Managed rules
                for managed_rule in properties.get("managedRules", {}).get("managedRuleSets", []):
                    fd_info["ManagedRulesetName"] = managed_rule.get("ruleSetType", '')
                    fd_info["ManagedRulesetVersion"] = managed_rule.get("ruleSetVersion", '')
                    for override in managed_rule.get("ruleGroupOverrides", []):
                        for rule in override.get("rules", []):
                            if rule.get("ruleId") in [100100, 100200, 100300]:
                                fd_info["BadBotsAntiBypass"] = "FAIL"
                            else:
                                fd_info["BadBotsAntiBypass"] = "PASS"
                    break  # Only consider first managed ruleset for now

            output_data.append(fd_info)

    # Export to CSV
    df = pd.DataFrame(output_data)
    df.to_csv("FrontDoorClassic.csv", index=False)
    print("CSV exported: FrontDoorClassic.csv")