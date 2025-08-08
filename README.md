# Databricks notebook compatible
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from typing import List, Dict, Any, Optional
import requests
import json
import logging
import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ------------------ Configurable Values ------------------
TENANT_ID = dbutils.secrets.get("graph", "tenant_id")
CLIENT_ID = dbutils.secrets.get("graph", "client_id")
CLIENT_SECRET = dbutils.secrets.get("graph", "client_secret")

INPUT_CATALOG_TABLE = "my_catalog.my_schema.group_ids_table"
OUTPUT_TABLE = "my_catalog.my_schema.group_members_output"

BATCH_SIZE = 20
MAX_WORKERS = 10
RATE_LIMIT_DELAY = 1.0
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 1.0
# ---------------------------------------------------------

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@dataclass
class GraphConfig:
    tenant_id: str
    client_id: str
    client_secret: str
    base_url: str = "https://graph.microsoft.com/v1.0"
    batch_size: int = BATCH_SIZE
    max_workers: int = MAX_WORKERS
    retry_attempts: int = RETRY_ATTEMPTS
    retry_backoff: float = RETRY_BACKOFF
    rate_limit_delay: float = RATE_LIMIT_DELAY

class GraphAPIError(Exception):
    pass

class MicrosoftGraphBatchProcessor:
    def __init__(self, config: GraphConfig):
        self.config = config
        self.access_token = None
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.retry_attempts,
            backoff_factor=self.config.retry_backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def authenticate(self):
        url = f"https://login.microsoftonline.com/{self.config.tenant_id}/oauth2/v2.0/token"
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.config.client_id,
            'client_secret': self.config.client_secret,
            'scope': 'https://graph.microsoft.com/.default'
        }
        response = self.session.post(url, data=data)
        if response.status_code != 200:
            raise GraphAPIError("Authentication failed")
        self.access_token = response.json()['access_token']
        logger.info("Authentication successful")

    def _get_headers(self) -> Dict[str, str]:
        if not self.access_token:
            raise GraphAPIError("Not authenticated")
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def _create_batch_request(self, group_ids: List[str], batch_id_start: int) -> Dict[str, Any]:
        return {
            "requests": [
                {
                    "id": str(batch_id_start + i),
                    "method": "GET",
                    "url": f"/groups/{gid}/members?$select=id,displayName,userPrincipalName,@odata.type"
                }
                for i, gid in enumerate(group_ids)
            ]
        }

    def _extract_object_type(self, odata_type: str) -> str:
        if not odata_type:
            return "Unknown"
        return odata_type.split("#microsoft.graph.")[-1] if "#microsoft.graph." in odata_type else odata_type

    def _process_batch_response(self, response_data: Dict[str, Any], group_ids: List[str], batch_id_start: int) -> List[Dict[str, Any]]:
        results = []
        for response in response_data.get("responses", []):
            try:
                request_id = int(response["id"])
                group_id = group_ids[request_id - batch_id_start]
                status = response.get("status", 0)

                if status == 200:
                    members = response["body"].get("value", [])
                    for member in members:
                        results.append({
                            "group_id": group_id,
                            "object_type": self._extract_object_type(member.get("@odata.type", "")),
                            "object_id": member.get("id", ""),
                            "object_name": member.get("displayName", member.get("userPrincipalName", ""))
                        })
                    if "@odata.nextLink" in response["body"]:
                        logger.warning(f"Pagination exists for group: {group_id} — additional pages will be skipped.")
                else:
                    logger.warning(f"Group {group_id} returned status {status}")
            except Exception as e:
                logger.error(f"Failed to process response: {e}")
        return results

    def _process_batch_chunk(self, group_ids_chunk: List[str], chunk_index: int) -> List[Dict[str, Any]]:
        try:
            batch_id_start = chunk_index * self.config.batch_size
            batch_request = self._create_batch_request(group_ids_chunk, batch_id_start)
            url = f"{self.config.base_url}/$batch"
            response = self.session.post(url, headers=self._get_headers(), json=batch_request, timeout=60)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "60"))
                logger.warning(f"Rate limited. Retrying after {retry_after} seconds.")
                time.sleep(retry_after)
                response = self.session.post(url, headers=self._get_headers(), json=batch_request, timeout=60)

            response.raise_for_status()
            return self._process_batch_response(response.json(), group_ids_chunk, batch_id_start)
        except Exception as e:
            logger.error(f"Error processing batch {chunk_index}: {e}")
            return []

    def fetch_group_members_batch(self, group_ids: List[str]) -> List[Dict[str, Any]]:
        chunks = [group_ids[i:i + self.config.batch_size] for i in range(0, len(group_ids), self.config.batch_size)]
        all_results = []
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(self._process_batch_chunk, chunk, i): i for i, chunk in enumerate(chunks)}
            for future in as_completed(futures):
                all_results.extend(future.result())
                time.sleep(self.config.rate_limit_delay)
        logger.info(f"Fetched members for {len(group_ids)} groups. Total records: {len(all_results)}")
        return all_results

    def create_spark_dataframe(self, group_ids: List[str], spark: SparkSession) -> DataFrame:
        if not self.access_token:
            self.authenticate()
        data = self.fetch_group_members_batch(group_ids)
        schema = StructType([
            StructField("group_id", StringType(), True),
            StructField("object_type", StringType(), True),
            StructField("object_id", StringType(), True),
            StructField("object_name", StringType(), True),
        ])
        return spark.createDataFrame(data, schema)

# ------------------ MAIN EXECUTION ------------------

# Start Spark
spark = SparkSession.builder.appName("GraphGroupMembers").getOrCreate()

# Step 1: Read group IDs
group_df = spark.read.table(INPUT_CATALOG_TABLE).select("group_id").distinct()
group_ids = [row["group_id"] for row in group_df.collect()]
logger.info(f"Loaded {len(group_ids)} group IDs from {INPUT_CATALOG_TABLE}")

# Step 2: Init processor
config = GraphConfig(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET
)
processor = MicrosoftGraphBatchProcessor(config)

# Step 3: Create Spark DataFrame with results
df_members = processor.create_spark_dataframe(group_ids, spark)

# Step 4: Display or Write
df_members.show(truncate=False)

# Optional: Write to Delta
df_members.write.mode("overwrite").format("delta").saveAsTable(OUTPUT_TABLE)
