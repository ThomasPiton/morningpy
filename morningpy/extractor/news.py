from datetime import datetime
import pandas as pd

from morningpy.core.client import BaseClient
from morningpy.core.base_extract import BaseExtractor
from morningpy.config.news import *
from morningpy.schema.news import *


class HeadlineNewsExtractor(BaseExtractor):
    
    config = HeadlineNewsConfig
    schema = HeadlineNewsSchema
    
    def __init__(self, edition, market, news):
     
        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)

        super().__init__(client)

        self.edition = edition
        self.market = market
        self.news = news
        self.url = self.config.API_URL
        self.params = self.config.PARAMS
        self.market_id = self.config.MARKET_ID
        self.edition_id = self.config.EDITION_ID
        self.endpoint_mapping = self.config.ENDPOINT
        
    def _check_inputs(self) -> None:
        pass

    def _build_request(self) -> None:
        edition_id = self.edition_id[self.edition]
        market_id = self.market_id[self.market]
        endpoint = self.endpoint_mapping[self.edition][self.news]
        self.url = f"{self.url}/{edition_id}/{endpoint}"
        self.params["marketID"]=market_id
        self.params["sectionFallBack"]=self.news
        
    def _process_response(self,response: dict) -> pd.DataFrame:

        def format_display_date(date_str: str) -> str:
            cleaned = date_str.replace("Z", "")
            try:
                dt = datetime.fromisoformat(cleaned)
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                return date_str
            
        if not isinstance(response, dict) or not response:
            return pd.DataFrame(columns=['news_type', 'Country', 'display_date', 'title', 'subtitle', 'Tags', 'link', 'language'])
        
        stories = response.get('page', {}).get('stories', [])
        rows = []

        for story in stories:
            display_date = format_display_date(story.get('displayDate', ''))
            title = story.get('headline', {}).get('title', '')
            subtitle = story.get('headline', {}).get('subtitle', '')
            tags = ','.join([section.get('name', '') for section in story.get('tags', [])])
            link = f"https://global.morningstar.com{story.get('canonicalURL', '')}"
            language = story.get('language', '')

            rows.append({
                'news': self.news,
                'market': self.market,
                'display_date': display_date,
                'title': title,
                'subtitle': subtitle,
                'tags': tags,
                'link': link,
                'language': language
            })

        df = pd.DataFrame(rows)
        return df
    