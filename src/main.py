# src/main.py
import asyncio
import os
import logging
from typing import List, Dict
from src.database.models import DatabaseManager
from src.scraping.scrapfly_client import ScrapflyService
from src.scraping.incremental_logic import IncrementalScraper

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TikTokIncrementalScraper:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.scrapfly_service = ScrapflyService()
        self.incremental_logic = IncrementalScraper(self.db_manager)
    
    async def scrape_profiles(self, usernames: List[str]) -> List[Dict]:
        """Main method to scrape multiple profiles with incremental logic"""
        results = []
        
        for username in usernames:
            logger.info(f"Processing profile: {username}")
            
            try:
                profile_data = await self.scrapfly_service.scrape_profile(username)
                
                if not profile_data:
                    results.append({
                        'username': username,
                        'error': 'Failed to scrape profile',
                        'success': False
                    })
                    continue
                
                profile_id = self.incremental_logic.get_or_create_profile(username, profile_data)
                
                should_create, change_analysis = self.incremental_logic.should_create_snapshot(
                    profile_id, profile_data
                )
                
                result = {
                    'profile_id': profile_id,
                    'username': username,
                    'should_create_snapshot': should_create,
                    'change_analysis': change_analysis,
                    'success': True
                }
                
                if should_create:
                    snapshot_id = self.incremental_logic.create_snapshot(
                        profile_id, profile_data, change_analysis
                    )
                    result['snapshot_id'] = snapshot_id
                    logger.info(f"Created new snapshot for {username}: {change_analysis['reason']}")
                else:
                    logger.info(f"No new snapshot needed for {username}")
                
                results.append(result)
                
                await asyncio.sleep(2)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Failed to process {username}: {e}")
                results.append({
                    'username': username,
                    'error': str(e),
                    'success': False
                })
        
        return results
    
    def get_stats(self) -> Dict:
        """Get scraping statistics"""
        with self.db_manager.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) as total FROM profiles")
            total_profiles = cur.fetchone()['total']
            
            cur.execute("SELECT COUNT(*) as total FROM profile_snapshots")
            total_snapshots = cur.fetchone()['total']
            
            cur.execute("SELECT COUNT(*) as changed FROM profile_snapshots WHERE change_detected = TRUE")
            changed_snapshots = cur.fetchone()['changed']
            
            return {
                'total_profiles': total_profiles,
                'total_snapshots': total_snapshots,
                'changed_snapshots': changed_snapshots
            }

async def main():
    """Main execution function"""
    scraper = TikTokIncrementalScraper()
    
    # Example profiles to scrape - replace with your target profiles
    profiles_to_scrape = [
        "tiktok",
        "example_user",
        "test_account"
    ]
    
    logger.info("Starting incremental TikTok scraping...")
    results = await scraper.scrape_profiles(profiles_to_scrape)
    
    stats = scraper.get_stats()
    logger.info(f"Scraping completed. Stats: {stats}")
    
    return results

if __name__ == "__main__":
    asyncio.run(main())
