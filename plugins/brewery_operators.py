from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests

class APIExtractorOperator(BaseOperator):
    """
    Extracts data from the Brewery API (OpenBreweryDB) in a paginated form.
    """
    @apply_defaults
    def __init__(self, api_url, params=None, *args, **kwargs):
        """
        :param api_url: The URL of the API endpoint to fetch breweries from.
        """
        super().__init__(*args, **kwargs)
        self.api_url = api_url
        self.params = params or {}

    def execute(self, context):
        """
        Executes the API call repeatedly using page and per_page parameters 
        until no more data is returned. Logs progress at each iteration.
        """
        self.log.info(f"Extracting data from API: {self.api_url}")
        all_data = []
        page = 1
        per_page = 50
        params = self.params.copy()

        while True:
            # Merge pagination details into current params
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