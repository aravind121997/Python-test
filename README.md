# Databricks notebook source
"""
Azure Security Information Ingestion Notebook
------------------------------------------
This notebook ingests security information from Microsoft Graph API 
based on groups information and uploads it to Azure Data Lake Storage Gen2.

Author: Your Name
Created: May 18, 2025
"""

# COMMAND ----------
# Import required libraries
import os
import json
import logging
import datetime
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import msal
import requests
import pandas as pd
import numpy as np
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

# COMMAND ----------
# Create Databricks widgets for dynamic parameters
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("AzureSecurityIngestion").getOrCreate()

# Add widgets for configuration
dbutils.widgets.text("tenant_id", "", "Tenant ID")
dbutils.widgets.text("client_id", "", "Client ID (Application ID)")
dbutils.widgets.text("client_secret", "", "Client Secret")
dbutils.widgets.text("graph_scopes", "https://graph.microsoft.com/.default", "Graph API Scopes")
dbutils.widgets.text("group_table_name", "", "Source Table Name (e.g., 'database.groups_table')")
dbutils.widgets.text("adls_account_name", "", "ADLS Gen2 Account Name")
dbutils.widgets.text("adls_container", "security-data", "ADLS Gen2 Container")
dbutils.widgets.text("max_workers", "5", "Max Worker Threads")

# COMMAND ----------
# Configure logging
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("AzureSecurityIngestion")

class AzureSecurityIngestion:
    """Main class for handling Azure security information ingestion."""
    
    def __init__(self):
        """
        Initialize the ingestion class using Databricks widgets.
        """
        # Get values from widgets
        self.tenant_id = dbutils.widgets.get("tenant_id")
        self.client_id = dbutils.widgets.get("client_id")
        self.client_secret = dbutils.widgets.get("client_secret")
        self.graph_scopes = dbutils.widgets.get("graph_scopes").split(",")
        self.group_table_name = dbutils.widgets.get("group_table_name")
        self.adls_account_name = dbutils.widgets.get("adls_account_name")
        self.adls_container = dbutils.widgets.get("adls_container")
        
        # Validate required parameters
        self._validate_parameters()
        
        # Initialize the Microsoft Graph client
        self.graph_client = self._initialize_graph_client()
        
        # Initialize the ADLS client
        self.adls_client = self._initialize_adls_client()
        
        # Set timestamp for output files
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize the Microsoft Graph client
        self.graph_client = self._initialize_graph_client()
        
        # Initialize the ADLS client
        self.adls_client = self._initialize_adls_client()
        
        # Set timestamp for output files
        self.timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def _validate_parameters(self):
        """
        Validate that all required parameters are provided.
        
        Raises:
            ValueError: If any required parameter is missing
        """
        required_params = {
            "tenant_id": self.tenant_id,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "group_table_name": self.group_table_name,
            "adls_account_name": self.adls_account_name
        }
        
        missing_params = [k for k, v in required_params.items() if not v]
        
        if missing_params:
            err_msg = f"Missing required parameters: {', '.join(missing_params)}"
            logger.error(err_msg)
            raise ValueError(err_msg)
            
    def _initialize_graph_client(self) -> Dict:
        """
        Initialize the Microsoft Graph API client using MSAL.
        
        Returns:
            Dict: Configured Graph client details
        """
        try:
            app = msal.ConfidentialClientApplication(
                client_id=self.client_id,
                authority=f"https://login.microsoftonline.com/{self.tenant_id}",
                client_credential=self.client_secret
            )
            
            # Acquire a token for the Graph API
            result = app.acquire_token_for_client(scopes=self.graph_scopes)
            
            if "access_token" in result:
                logger.info("Successfully acquired token for Graph API")
                return {
                    "token": result["access_token"],
                    "app": app
                }
            else:
                logger.error(f"Failed to acquire token: {result.get('error')}")
                logger.error(f"Error description: {result.get('error_description')}")
                raise Exception("Failed to acquire token for Graph API")
        except Exception as e:
            logger.error(f"Failed to initialize Graph client: {str(e)}")
            raise
    
    def _initialize_adls_client(self) -> DataLakeServiceClient:
        """
        Initialize the Azure Data Lake Storage Gen2 client.
        
        Returns:
            DataLakeServiceClient: ADLS Gen2 client
        """
        try:
            credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            
            service_client = DataLakeServiceClient(
                account_url=f"https://{self.adls_account_name}.dfs.core.windows.net",
                credential=credential
            )
            
            logger.info("Successfully initialized ADLS Gen2 client")
            return service_client
        except Exception as e:
            logger.error(f"Failed to initialize ADLS client: {str(e)}")
            raise
    
    def fetch_groups_from_table(self) -> pd.DataFrame:
        """
        Fetch groups information from the configured Spark table.
        
        Returns:
            pd.DataFrame: DataFrame containing groups information
        """
        try:
            # Use Spark SQL to read from the table
            logger.info(f"Fetching groups from table {self.group_table_name}")
            
            query = f"SELECT * FROM {self.group_table_name}"
            groups_df = spark.sql(query).toPandas()
            
            # Validate the dataframe
            required_columns = ['group_id', 'group_name']
            for col in required_columns:
                if col not in groups_df.columns:
                    raise ValueError(f"Required column '{col}' not found in groups table")
            
            logger.info(f"Successfully fetched {len(groups_df)} groups")
            return groups_df
        except Exception as e:
            logger.error(f"Failed to fetch groups from table: {str(e)}")
            raise
    
    def validate_group(self, group_id: str) -> bool:
        """
        Validate if a group exists in Azure AD.
        
        Args:
            group_id: Group ID to validate
            
        Returns:
            bool: True if group is valid, False otherwise
        """
        try:
            url = f"https://graph.microsoft.com/v1.0/groups/{group_id}"
            headers = {
                'Authorization': f"Bearer {self.graph_client['token']}",
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                logger.info(f"Group {group_id} is valid")
                return True
            else:
                logger.warning(f"Group {group_id} is not valid. Status code: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error validating group {group_id}: {str(e)}")
            return False
    
    def get_group_owners(self, group_id: str) -> List[Dict]:
        """
        Get owners of a specific group.
        
        Args:
            group_id: ID of the group
            
        Returns:
            List[Dict]: List of owner details
        """
        try:
            url = f"https://graph.microsoft.com/v1.0/groups/{group_id}/owners"
            headers = {
                'Authorization': f"Bearer {self.graph_client['token']}",
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                owners = response.json().get('value', [])
                logger.info(f"Successfully fetched {len(owners)} owners for group {group_id}")
                return owners
            else:
                logger.warning(f"Failed to get owners for group {group_id}. Status code: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error getting owners for group {group_id}: {str(e)}")
            return []
    
    def get_group_members(self, group_id: str) -> List[Dict]:
        """
        Get members of a specific group.
        
        Args:
            group_id: ID of the group
            
        Returns:
            List[Dict]: List of member details
        """
        try:
            url = f"https://graph.microsoft.com/v1.0/groups/{group_id}/members"
            headers = {
                'Authorization': f"Bearer {self.graph_client['token']}",
                'Content-Type': 'application/json'
            }
            
            all_members = []
            next_link = url
            
            while next_link:
                response = requests.get(next_link, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    members = data.get('value', [])
                    all_members.extend(members)
                    next_link = data.get('@odata.nextLink')
                else:
                    logger.warning(f"Failed to get members for group {group_id}. Status code: {response.status_code}")
                    break
            
            logger.info(f"Successfully fetched {len(all_members)} members for group {group_id}")
            return all_members
        except Exception as e:
            logger.error(f"Error getting members for group {group_id}: {str(e)}")
            return []
    
    def get_service_principals(self, group_id: str) -> List[Dict]:
        """
        Get service principals from a specific group.
        
        Args:
            group_id: ID of the group
            
        Returns:
            List[Dict]: List of service principal details
        """
        try:
            members = self.get_group_members(group_id)
            service_principals = [m for m in members if m.get('@odata.type') == '#microsoft.graph.servicePrincipal']
            
            logger.info(f"Successfully identified {len(service_principals)} service principals in group {group_id}")
            return service_principals
        except Exception as e:
            logger.error(f"Error getting service principals for group {group_id}: {str(e)}")
            return []
    
    def upload_to_adls(self, data: Dict, group_id: str, data_type: str) -> bool:
        """
        Upload data to Azure Data Lake Storage Gen2.
        
        Args:
            data: Data to upload
            group_id: ID of the group the data belongs to
            data_type: Type of data (owners, members, service_principals)
            
        Returns:
            bool: True if upload is successful, False otherwise
        """
        try:
            # Create a JSON string
            json_data = json.dumps(data, indent=2)
            
            # Define the file path in ADLS
            file_path = f"security_data/{self.timestamp}/group_{group_id}/{data_type}.json"
            
            # Get the file system client
            file_system_client = self.adls_client.get_file_system_client(file_system=self.adls_container)
            
            # Get the directory client
            directory_client = file_system_client.get_directory_client(f"security_data/{self.timestamp}/group_{group_id}")
            
            # Create directory if it doesn't exist
            if not directory_client.exists():
                directory_client.create_directory()
            
            # Get the file client
            file_client = file_system_client.get_file_client(file_path)
            
            # Upload the data
            file_client.upload_data(json_data, overwrite=True)
            
            logger.info(f"Successfully uploaded {data_type} data for group {group_id} to {file_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {data_type} data for group {group_id}: {str(e)}")
            return False
    
    def process_group(self, group_row: pd.Series) -> Dict:
        """
        Process a single group to fetch and upload security information.
        
        Args:
            group_row: Row from the groups DataFrame
            
        Returns:
            Dict: Results of processing
        """
        group_id = group_row['group_id']
        group_name = group_row['group_name']
        
        logger.info(f"Processing group: {group_name} ({group_id})")
        
        result = {
            'group_id': group_id,
            'group_name': group_name,
            'is_valid': False,
            'owners_uploaded': False,
            'members_uploaded': False,
            'service_principals_uploaded': False
        }
        
        # Validate the group
        is_valid = self.validate_group(group_id)
        result['is_valid'] = is_valid
        
        if not is_valid:
            logger.warning(f"Skipping invalid group: {group_name} ({group_id})")
            return result
        
        # Get and upload owners
        owners = self.get_group_owners(group_id)
        if owners:
            owners_data = {
                'group_id': group_id,
                'group_name': group_name,
                'timestamp': self.timestamp,
                'owners': owners
            }
            result['owners_uploaded'] = self.upload_to_adls(owners_data, group_id, 'owners')
            result['owners_count'] = len(owners)
        
        # Get and upload members
        members = self.get_group_members(group_id)
        if members:
            members_data = {
                'group_id': group_id,
                'group_name': group_name,
                'timestamp': self.timestamp,
                'members': members
            }
            result['members_uploaded'] = self.upload_to_adls(members_data, group_id, 'members')
            result['members_count'] = len(members)
        
        # Get and upload service principals
        service_principals = self.get_service_principals(group_id)
        if service_principals:
            sp_data = {
                'group_id': group_id,
                'group_name': group_name,
                'timestamp': self.timestamp,
                'service_principals': service_principals
            }
            result['service_principals_uploaded'] = self.upload_to_adls(sp_data, group_id, 'service_principals')
            result['service_principals_count'] = len(service_principals)
        
        logger.info(f"Completed processing group: {group_name} ({group_id})")
        return result
    
    def run(self) -> Dict:
        """
        Run the full ingestion process.
        
        Returns:
            Dict: Summary of the ingestion process
        """
        start_time = datetime.datetime.now()
        logger.info(f"Starting Azure security information ingestion at {start_time}")
        
        # Get max_workers from widget
        max_workers = int(dbutils.widgets.get("max_workers"))
        
        # Fetch groups from table
        groups_df = self.fetch_groups_from_table()
        
        # Process each group in parallel
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_group = {executor.submit(self.process_group, row): row for _, row in groups_df.iterrows()}
            for future in as_completed(future_to_group):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    group = future_to_group[future]
                    logger.error(f"Exception processing group {group['group_id']}: {str(e)}")
        
        # Generate summary
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        valid_groups = sum(1 for r in results if r['is_valid'])
        owners_uploaded = sum(1 for r in results if r.get('owners_uploaded', False))
        members_uploaded = sum(1 for r in results if r.get('members_uploaded', False))
        service_principals_uploaded = sum(1 for r in results if r.get('service_principals_uploaded', False))
        
        total_owners = sum(r.get('owners_count', 0) for r in results)
        total_members = sum(r.get('members_count', 0) for r in results)
        total_service_principals = sum(r.get('service_principals_count', 0) for r in results)
        
        summary = {
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration,
            'total_groups': len(groups_df),
            'valid_groups': valid_groups,
            'invalid_groups': len(groups_df) - valid_groups,
            'owners_uploaded': owners_uploaded,
            'members_uploaded': members_uploaded,
            'service_principals_uploaded': service_principals_uploaded,
            'total_owners': total_owners,
            'total_members': total_members,
            'total_service_principals': total_service_principals
        }
        
        # Upload summary to ADLS
        summary_path = f"security_data/{self.timestamp}/summary.json"
        file_system_client = self.adls_client.get_file_system_client(file_system=self.adls_container)
        file_client = file_system_client.get_file_client(summary_path)
        file_client.upload_data(json.dumps(summary, indent=2), overwrite=True)
        
        logger.info(f"Ingestion completed. Summary uploaded to {summary_path}")
        logger.info(f"Summary: Processed {len(groups_df)} groups, {valid_groups} valid, "
                   f"{total_owners} owners, {total_members} members, {total_service_principals} service principals")
        
        # Create a Spark DataFrame with the results for visualization
        results_df = spark.createDataFrame(results)
        display(results_df)
        
        # Create a Spark DataFrame with the summary for visualization
        summary_rows = [(k, str(v)) for k, v in summary.items() if k != 'results']
        summary_df = spark.createDataFrame(summary_rows, ["Metric", "Value"])
        display(summary_df)
        
        return summary

# COMMAND ----------
# Main execution

try:
    # Instantiate and run the ingestion
    ingestion = AzureSecurityIngestion()
    summary = ingestion.run()
    
    # Print completion message
    print("Ingestion completed successfully. See summary above.")
    
except Exception as e:
    logger.error(f"Ingestion failed: {str(e)}")
    print(f"Ingestion failed: {str(e)}")
    raise
        print(