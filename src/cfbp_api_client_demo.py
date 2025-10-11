# Import required libraries
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from cfg_demo import COMPANY_CONFIG

# Setup logging for better visibility
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class CFPBAPIClient:
    """Client for accessing the CFPB Consumer Complaint Database API."""

    BASE_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
    DEFAULT_TIMEOUT = 30
    MAX_RETRIES = 3

    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        """Initialize the CFPB API client."""
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic and proper headers."""
        session = requests.Session()

        # CRITICAL FIX: Add User-Agent header (CFPB API requires this!)
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; ConsumerComplaintETL/1.0; Python/requests)",
                "Accept": "application/json",
            }
        )

        retry_strategy = Retry(
            total=self.MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get_complaints(
        self,
        date_received_min: Optional[str] = None,
        date_received_max: Optional[str] = None,
        size: int = 10000,
        frm: int = 0,
        sort: str = "created_date_desc",
        search_term: Optional[str] = None,
        search_field: Optional[str] = None,
        no_aggs: bool = False,
        **filters,
    ) -> Dict[str, Any]:
        """Fetch consumer complaints from the CFPB API with better error handling."""
        params = {"size": min(size, 10000), "frm": frm, "sort": sort, "format": "json"}

        if date_received_min:
            params["date_received_min"] = date_received_min
        if date_received_max:
            params["date_received_max"] = date_received_max

        # Add search parameters for company-specific queries
        if search_term:
            params["search_term"] = search_term
        if search_field:
            params["field"] = search_field
        if no_aggs:
            params["no_aggs"] = "true"

        params.update(filters)

        try:
            print(f"Requesting: {self.BASE_URL}")
            print(f"Params: {params}")

            response = self.session.get(self.BASE_URL, params=params, timeout=self.timeout)

            print(f"Status Code: {response.status_code}")

            if response.status_code != 200:
                print(f"ERROR - Response: {response.text[:500]}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e}")
            print(f"Response: {e.response.text[:500] if e.response else 'No response'}")
            raise
        except Exception as e:
            print(f"Unexpected error: {type(e).__name__}: {e}")
            raise

    def get_complaints_list(
        self,
        date_received_min: Optional[str] = None,
        date_received_max: Optional[str] = None,
        max_records: Optional[int] = None,
        **filters,
    ) -> List[Dict[str, Any]]:
        """Fetch complaints and return as a list of dictionaries."""
        response = self.get_complaints(
            date_received_min=date_received_min,
            date_received_max=date_received_max,
            size=max_records or 1000,
            **filters,
        )

        # Handle different response formats
        if isinstance(response, list):
            # Direct list format (newer API response)
            complaints = [hit.get("_source", {}) for hit in response]
        elif isinstance(response, dict) and "hits" in response:
            # Nested dict format (older API response)
            hits = response.get("hits", {}).get("hits", [])
            complaints = [hit.get("_source", {}) for hit in hits]
        else:
            complaints = []

        return complaints

    def get_complaints_last_n_days(
        self, days: int = 7, max_records: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Fetch complaints from the last N days."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        return self.get_complaints_list(
            date_received_min=start_date.strftime("%Y-%m-%d"),
            date_received_max=end_date.strftime("%Y-%m-%d"),
            max_records=max_records,
        )

    def get_complaints_by_company(
        self,
        company_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_records: Optional[int] = None,
        no_aggs: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Fetch complaints for a specific company.

        Args:
            company_name: Name of the company to search for (e.g., 'jpmorgan')
            start_date: Start date (YYYY-MM-DD), defaults to '2011-12-01'
            end_date: End date (YYYY-MM-DD), defaults to today
            max_records: Maximum total records to fetch (None for all available)
            no_aggs: Disable aggregations for faster responses (default: True)

        Returns:
            List of complaint records for the specified company
        """
        # Set default dates if not provided
        if not start_date:
            start_date = "2011-12-01"  # CFPB database start date
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")

        print(f"Searching for complaints from {company_name}")
        print(f"Date range: {start_date} to {end_date}")

        response = self.get_complaints(
            date_received_min=start_date,
            date_received_max=end_date,
            search_term=company_name,
            search_field="company",
            no_aggs=no_aggs,
            size=max_records or 10000,
        )

        # Handle different response formats
        if isinstance(response, list):
            # Direct list format (newer API response)
            complaints = [hit.get("_source", {}) for hit in response]
            total_available = len(complaints)
        elif isinstance(response, dict) and "hits" in response:
            # Nested dict format (older API response)
            hits = response.get("hits", {}).get("hits", [])
            complaints = [hit.get("_source", {}) for hit in hits]
            total_value = response.get("hits", {}).get("total", {})
            if isinstance(total_value, dict):
                total_available = total_value.get("value", 0)
            else:
                total_available = total_value
        else:
            complaints = []
            total_available = 0

        print(f"Total complaints available: {total_available:,}")
        print(f"Fetched: {len(complaints)} complaints")

        return complaints

    def close(self):
        """Close the API client session."""
        if self.session:
            self.session.close()


# Initialize the client
client = CFPBAPIClient()
print(f"Base URL: {client.BASE_URL}")

# Create data directory if it doesn't exist
data_dir = "data"
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Loop through config and fetch complaints for each company
for config in COMPANY_CONFIG:
    print(f"\nFetching complaints for {config['company_name']}...")
    print("=" * 80)

    company_data = client.get_complaints_by_company(
        company_name=config["company_name"],
        start_date=config["start_date"],
        end_date=config["end_date"],
    )

    if company_data:
        df_company = pd.DataFrame(company_data)
        print(f"✓ Fetched {len(df_company)} complaints")

        # Save to CSV with sanitized filename
        filename = config["company_name"].replace(" ", "_").lower()
        output_file = os.path.join(data_dir, f"{filename}_complaints.csv")
        df_company.to_csv(output_file, index=False)
        print(f"✓ Saved to {output_file}")
    else:
        print(f"⚠ No data returned for {config['company_name']}")

print("\n" + "=" * 80)
print("✓ All companies processed")
