
import os
import uuid
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

# Azure-specific imports
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

class ADLSFileStager:
    def __init__(
        self, 
        tenant_id: str,
        client_id: str,
        client_secret: str,
        account_name: str,
        staging_filesystem: str,
        default_filesystem: str,
        root_staging_folder: str = 'staging',
        root_default_folder: str = 'processed'
    ):
        """
        Initialize the ADLSFileStager with Azure Service Principal authentication.
        
        :param tenant_id: Azure Active Directory tenant ID
        :param client_id: Service Principal's application (client) ID
        :param client_secret: Service Principal's client secret
        :param account_name: Azure Data Lake Storage account name
        :param staging_filesystem: Filesystem (container) for staging
        :param default_filesystem: Filesystem (container) for final storage
        :param root_staging_folder: Root folder for staging files
        :param root_default_folder: Root folder for processed files
        """
        # Configure logging
        self._configure_logging()
        
        # Store authentication and configuration parameters
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.account_name = account_name
        
        # Filesystem and folder configurations
        self.staging_filesystem = staging_filesystem
        self.default_filesystem = default_filesystem
        self.root_staging_folder = root_staging_folder
        self.root_default_folder = root_default_folder
        
        # Authenticate and create Data Lake Service Client
        self.data_lake_service_client = self._create_data_lake_service_client()
        
        # Logging
        self.logger = logging.getLogger(__name__)

    def _create_data_lake_service_client(self) -> DataLakeServiceClient:
        """
        Create a DataLakeServiceClient using Service Principal authentication.
        
        :return: Authenticated DataLakeServiceClient
        """
        try:
            # Create credentials using ClientSecretCredential
            credential = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret
            )
            
            # Construct the account URL
            account_url = f"https://{self.account_name}.dfs.core.windows.net"
            
            # Create and return the service client
            return DataLakeServiceClient(
                account_url=account_url, 
                credential=credential
            )
        except Exception as auth_error:
            logging.error(f"Authentication failed: {str(auth_error)}")
            raise

    def _configure_logging(self):
        """
        Configure comprehensive logging for file staging operations.
        """
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('adls_file_stager.log', mode='a')
            ]
        )

    def stage_files(
        self, 
        local_source_path: str, 
        file_pattern: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Stage files from local storage to ADLS staging folder.
        
        :param local_source_path: Local directory containing files to stage
        :param file_pattern: Optional file pattern to filter files
        :return: Staging operation metadata
        """
        try:
            # Generate timestamp for staging folder
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            staging_folder = f"{self.root_staging_folder}/{timestamp}"
            
            # Get staging filesystem and directory clients
            staging_fs_client = self.data_lake_service_client.get_file_system_client(self.staging_filesystem)
            staging_dir_client = staging_fs_client.get_directory_client(staging_folder)
            
            # Create staging directory
            try:
                staging_dir_client.create_directory()
            except ResourceExistsError:
                self.logger.warning(f"Staging directory {staging_folder} already exists")
            
            # Track copied files
            copied_files = []
            
            # Iterate and copy files
            for filename in os.listdir(local_source_path):
                # Apply file pattern filter if specified
                if file_pattern and not self._matches_pattern(filename, file_pattern):
                    continue
                
                local_file_path = os.path.join(local_source_path, filename)
                
                # Skip directories
                if os.path.isdir(local_file_path):
                    continue
                
                try:
                    # Create file client for destination
                    dest_file_client = staging_dir_client.get_file_client(filename)
                    
                    # Upload file to staging
                    with open(local_file_path, 'rb') as file_data:
                        dest_file_client.upload_data(file_data, overwrite=True)
                    
                    copied_files.append(filename)
                    self.logger.info(f"Staged file: {filename}")
                
                except Exception as file_upload_error:
                    self.logger.error(f"Error staging file {filename}: {str(file_upload_error)}")
            
            # Create success file
            self._create_success_file(staging_dir_client)
            
            # Prepare staging metadata
            staging_metadata = {
                'timestamp': timestamp,
                'staging_path': staging_folder,
                'copied_files': copied_files,
                'total_files': len(copied_files)
            }
            
            return staging_metadata
        
        except Exception as e:
            self.logger.error(f"Staging process failed: {str(e)}")
            raise

    def process_staged_files(self, staging_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process staged files by moving to default storage with structured path.
        
        :param staging_metadata: Metadata from staging operation
        :return: Processing operation metadata
        """
        try:
            # Generate target path with year/month/day/uuid
            now = datetime.now()
            target_folder = (
                f"{self.root_default_folder}/"
                f"{now.year}/{now.month:02d}/{now.day:02d}/{str(uuid.uuid4())}"
            )
            
            # Get filesystem clients
            staging_fs_client = self.data_lake_service_client.get_file_system_client(self.staging_filesystem)
            default_fs_client = self.data_lake_service_client.get_file_system_client(self.default_filesystem)
            
            # Create target directory in default filesystem
            target_dir_client = default_fs_client.get_directory_client(target_folder)
            target_dir_client.create_directory()
            
            # Get staging directory
            staging_dir_client = staging_fs_client.get_directory_client(staging_metadata['staging_path'])
            
            # Track processed files
            processed_files = []
            
            # Iterate and move files
            for filename in staging_metadata['copied_files']:
                try:
                    # Source file client
                    source_file_client = staging_dir_client.get_file_client(filename)
                    
                    # Destination file client
                    dest_file_client = target_dir_client.get_file_client(filename)
                    
                    # Download source file contents
                    file_contents = source_file_client.download_file().readall()
                    
                    # Upload to default storage
                    dest_file_client.upload_data(file_contents, overwrite=True)
                    
                    # Delete from staging
                    source_file_client.delete_file()
                    
                    processed_files.append(filename)
                    self.logger.info(f"Processed file: {filename}")
                
                except Exception as file_process_error:
                    self.logger.error(f"Error processing file {filename}: {str(file_process_error)}")
            
            # Prepare processing metadata
            processing_metadata = {
                'target_path': target_folder,
                'processed_files': processed_files,
                'total_processed_files': len(processed_files)
            }
            
            return processing_metadata
        
        except Exception as e:
            self.logger.error(f"File processing failed: {str(e)}")
            raise

    def _create_success_file(self, directory_client):
        """
        Create a success marker file in the given directory.
        
        :param directory_client: Directory to create success file in
        """
        try:
            success_file_client = directory_client.get_file_client('_SUCCESS')
            success_file_client.upload_data(
                f"Staging completed at {datetime.now().isoformat()}".encode('utf-8'),
                overwrite=True
            )
            self.logger.info("Success file created")
        except Exception as e:
            self.logger.error(f"Failed to create success file: {str(e)}")

    def _matches_pattern(self, filename: str, pattern: str) -> bool:
        """
        Check if filename matches the given pattern.
        
        :param filename: Name of the file
        :param pattern: File pattern to match (e.g., '*.csv')
        :return: True if filename matches pattern, False otherwise
        """
        import fnmatch
        return fnmatch.fnmatch(filename, pattern)

# Example usage
def example_usage():
    """
    Demonstrates how to use ADLSFileStager with Service Principal authentication.
    
    Prerequisites:
    1. Install required packages:
       pip install azure-identity azure-storage-filedatalake
    
    2. Obtain Service Principal credentials from Azure Portal
    """
    try:
        # Initialize file stager with Service Principal credentials
        file_stager = ADLSFileStager(
            tenant_id='your-tenant-id',
            client_id='your-client-id',
            client_secret='your-client-secret',
            account_name='your-storage-account-name',
            staging_filesystem='landing',
            default_filesystem='processed'
        )
        
        # Stage files from local source
        staging_result = file_stager.stage_files(
            local_source_path="/path/to/local/files",
            file_pattern="*.csv"  # Optional: stage only CSV files
        )
        
        # Process staged files
        processing_result = file_stager.process_staged_files(staging_result)
        
        # Log results
        print("Staging complete:", staging_result)
        print("Processing complete:", processing_result)
    
    except Exception as e:
        print(f"An error occurred: {e}")

# Allow direct script execution
if __name__ == "__main__":
    example_usage()