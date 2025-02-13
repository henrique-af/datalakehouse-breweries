from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests

class APIExtractorOperator(BaseOperator):
    @apply_defaults
    def __init__(self, api_url, params=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_url = api_url
        self.params = params or {}

    def execute(self, context):
        self.log.info(f"Extracting data from API: {self.api_url}")
        all_data = []
        page = 1
        per_page = 50
        params = self.params.copy()

        while True:
            params.update({'page': page, 'per_page': per_page})
            response = requests.get(self.api_url, params=params)

            if response.status_code != 200:
                self.log.error(f"Failed to fetch data: {response.status_code}")
                break
            
            data = response.json()
            if not data:
                break
            
            all_data.extend(data)
            self.log.info(f"Total items fetched: {len(all_data)}")

            # If less than 'per_page' items are returned, assume no more pages
            if len(data) < per_page:
                break
            page += 1

        return all_data