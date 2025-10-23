# src/scraping/incremental_logic.py
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import logging
from src.database.models import DatabaseManager

logger = logging.getLogger(__name__)

class IncrementalScraper:
    def __init__(self, db_manager: DatabaseManager, change_threshold: float = 0.01):
        self.db = db_manager
        self.change_threshold = change_threshold
    
    def get_or_create_profile(self, username: str, profile_data: Dict) -> int:
        """Get existing profile ID or create new profile"""
        with self.db.get_cursor(commit=True) as cur:
            cur.execute(
                "SELECT id FROM profiles WHERE username = %s",
                (username.lower(),)
            )
            result = cur.fetchone()
            
            if result:
                profile_id = result['id']
                cur.execute(
                    "UPDATE profiles SET last_checked = %s WHERE id = %s",
                    (datetime.now(), profile_id)
                )
                return profile_id
            else:
                cur.execute(
                    """
                    INSERT INTO profiles 
                    (username, profile_id, display_name, bio, avatar_url, verified, last_checked)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        username.lower(),
                        profile_data.get('profile_id'),
                        profile_data.get('display_name'),
                        profile_data.get('bio'),
                        profile_data.get('avatar_url'),
                        profile_data.get('verified', False),
                        datetime.now()
                    )
                )
                new_profile = cur.fetchone()
                return new_profile['id']
    
    def get_last_snapshot(self, profile_id: int) -> Optional[Dict]:
        """Get the most recent snapshot for a profile"""
        with self.db.get_cursor() as cur:
            cur.execute(
                """
                SELECT * FROM profile_snapshots 
                WHERE profile_id = %s 
                ORDER BY snapshot_timestamp DESC 
                LIMIT 1
                """,
                (profile_id,)
            )
            return cur.fetchone()
    
    def calculate_changes(self, current_data: Dict, last_snapshot: Dict) -> Tuple[bool, Dict]:
        """Calculate changes between current data and last snapshot"""
        changes = {}
        has_changed = False
        
        metrics = ['followers_count', 'following_count', 'likes_count', 'video_count']
        
        for metric in metrics:
            current_val = current_data.get(metric, 0)
            last_val = last_snapshot.get(metric, 0)
            
            if last_val == 0 and current_val > 0:
                changes[metric] = {
                    'old_value': last_val,
                    'new_value': current_val,
                    'absolute_change': current_val,
                    'percentage_change': 100.0
                }
                has_changed = True
            elif last_val > 0:
                absolute_change = current_val - last_val
                percentage_change = (absolute_change / last_val) * 100
                
                if abs(percentage_change) >= (self.change_threshold * 100):
                    changes[metric] = {
                        'old_value': last_val,
                        'new_value': current_val,
                        'absolute_change': absolute_change,
                        'percentage_change': percentage_change
                    }
                    has_changed = True
        
        return has_changed, changes
    
    def should_create_snapshot(self, profile_id: int, current_data: Dict) -> Tuple[bool, Dict]:
        """Determine if we should create a new snapshot"""
        last_snapshot = self.get_last_snapshot(profile_id)
        
        if not last_snapshot:
            return True, {'reason': 'first_snapshot'}
        
        has_changed, changes = self.calculate_changes(current_data, last_snapshot)
        
        if has_changed:
            return True, {
                'reason': 'metrics_changed',
                'changes': changes,
                'previous_snapshot_id': last_snapshot['id']
            }
        
        last_timestamp = last_snapshot['snapshot_timestamp']
        time_since_last = datetime.now() - last_timestamp
        
        if time_since_last > timedelta(days=7):
            return True, {
                'reason': 'periodic_snapshot',
                'days_since_last': time_since_last.days
            }
        
        return False, {'reason': 'no_significant_changes'}
    
    def create_snapshot(self, profile_id: int, profile_data: Dict, change_analysis: Dict) -> int:
        """Create a new snapshot in the database"""
        with self.db.get_cursor(commit=True) as cur:
            cur.execute(
                """
                INSERT INTO profile_snapshots 
                (profile_id, followers_count, following_count, likes_count, 
                 video_count, change_detected, previous_snapshot_id, raw_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    profile_id,
                    profile_data.get('followers_count'),
                    profile_data.get('following_count'),
                    profile_data.get('likes_count'),
                    profile_data.get('video_count'),
                    change_analysis['reason'] != 'no_significant_changes',
                    change_analysis.get('previous_snapshot_id'),
                    profile_data.get('raw_data')
                )
            )
            snapshot = cur.fetchone()
            logger.info(f"Created snapshot {snapshot['id']} for profile {profile_id}")
            return snapshot['id']
