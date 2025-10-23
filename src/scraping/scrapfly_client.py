# src/scraping/scrapfly_client.py
import os
from scrapfly import ScrapeConfig, ScrapflyClient, ScrapeApiResponse
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class ScrapflyService:
    def __init__(self):
        self.api_key = os.getenv('SCRAPFLY_API_KEY')
        if not self.api_key:
            raise ValueError("SCRAPFLY_API_KEY environment variable is required")
        
        self.client = ScrapflyClient(key=self.api_key)
        self.base_config = {
            "asp": True,  # Anti-scraping protection
            "country": "US",  # Proxy country
            "render_js": True,  # JavaScript rendering
            "proxy_pool": "public_residential_pool"  # Use residential proxies
        }
    
    async def scrape_profile(self, username: str) -> Optional[Dict]:
        """Scrape TikTok profile data using Scrapfly"""
        try:
            url = f"https://www.tiktok.com/@{username.lstrip('@')}"
            
            response: ScrapeApiResponse = await self.client.async_scrape(
                ScrapeConfig(url, **self.base_config)
            )
            
            if response.success:
                return self._parse_profile_data(response, username)
            else:
                logger.error(f"Scrapfly error for {username}: {response.scrape_result['error']}")
                return None
                
        except Exception as e:
            logger.error(f"Error scraping profile {username}: {e}")
            return None
    
    def _parse_profile_data(self, response: ScrapeApiResponse, username: str) -> Dict:
        """Parse profile data from Scrapfly response"""
        try:
            selector = response.selector
            script_data = selector.xpath("//script[@id='__UNIVERSAL_DATA_FOR_REHYDRATION__']/text()").get()
            
            if not script_data:
                raise ValueError("No universal data found")
            
            import json
            data = json.loads(script_data)
            user_info = data["__DEFAULT_SCOPE__"]["webapp.user-detail"]["userInfo"]["user"]
            
            return {
                'username': username,
                'profile_id': user_info.get('id'),
                'display_name': user_info.get('nickname'),
                'bio': user_info.get('signature'),
                'avatar_url': user_info.get('avatarLarger'),
                'verified': user_info.get('verified', False),
                'followers_count': user_info.get('followerCount', 0),
                'following_count': user_info.get('followingCount', 0),
                'likes_count': user_info.get('heartCount', 0),
                'video_count': user_info.get('videoCount', 0),
                'raw_data': user_info
            }
            
        except Exception as e:
            logger.error(f"Error parsing profile data for {username}: {e}")
            raise
