# src/database/models.py
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        
    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(self.database_url)
            yield conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_cursor(self, commit=False):
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            try:
                yield cursor
                if commit:
                    conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Database cursor error: {e}")
                raise
            finally:
                cursor.close()

    def init_db(self):
        """Initialize database tables"""
        with self.get_cursor(commit=True) as cur:
            # Create profiles table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS profiles (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(100) UNIQUE NOT NULL,
                    profile_id VARCHAR(100) UNIQUE,
                    display_name VARCHAR(200),
                    bio TEXT,
                    avatar_url TEXT,
                    verified BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    last_checked TIMESTAMP WITH TIME ZONE,
                    is_active BOOLEAN DEFAULT TRUE
                )
            """)
            
            # Create profile_snapshots table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS profile_snapshots (
                    id SERIAL PRIMARY KEY,
                    profile_id INTEGER REFERENCES profiles(id),
                    followers_count INTEGER,
                    following_count INTEGER,
                    likes_count INTEGER,
                    video_count INTEGER,
                    snapshot_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    change_detected BOOLEAN DEFAULT FALSE,
                    previous_snapshot_id INTEGER REFERENCES profile_snapshots(id),
                    raw_data JSONB
                )
            """)
            
            # Create indexes
            cur.execute("CREATE INDEX IF NOT EXISTS idx_profiles_username ON profiles(username)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_profile_id ON profile_snapshots(profile_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp ON profile_snapshots(snapshot_timestamp)")
            
            logger.info("Database tables initialized successfully")
