# src/web_app.py
import os
import asyncio
import logging
from flask import Flask, render_template, request, jsonify, session
from src.database.models import DatabaseManager
from src.scraping.scrapfly_client import ScrapflyService
from src.scraping.incremental_logic import IncrementalScraper
import json
from datetime import datetime

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET', 'dev-secret-key')

# Initialize components
db_manager = DatabaseManager()
scrapfly_service = ScrapflyService()
incremental_scraper = IncrementalScraper(db_manager)

@app.route('/')
def index():
    """Main page with profile input form"""
    return render_template('index.html')

@app.route('/profiles', methods=['POST'])
def add_profiles():
    """Add new profiles to monitor"""
    try:
        profiles_text = request.json.get('profiles', '')
        
        # Parse profiles (support comma, newline, or space separation)
        profiles = []
        for line in profiles_text.split('\n'):
            line_profiles = [p.strip().lstrip('@') for p in line.replace(',', ' ').split() if p.strip()]
            profiles.extend(line_profiles)
        
        profiles = list(set(profiles))  # Remove duplicates
        
        if not profiles:
            return jsonify({'success': False, 'error': 'No valid profiles provided'})
        
        # Store profiles in session (in production, use database)
        session['monitored_profiles'] = profiles
        session.modified = True
        
        return jsonify({
            'success': True, 
            'message': f'Added {len(profiles)} profiles to monitor',
            'profiles': profiles
        })
        
    except Exception as e:
        logging.error(f"Error adding profiles: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/scrape', methods=['POST'])
def scrape_profiles():
    """Run scraping on monitored profiles"""
    try:
        profiles = session.get('monitored_profiles', [])
        
        if not profiles:
            return jsonify({
                'success': False, 
                'error': 'No profiles to scrape. Add profiles first.'
            })
        
        # Run scraping asynchronously
        results = asyncio.run(run_scraping(profiles))
        
        return jsonify({
            'success': True,
            'message': f'Scraped {len(results)} profiles',
            'results': results,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logging.error(f"Error scraping profiles: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/status')
def get_status():
    """Get scraping status and statistics"""
    try:
        with db_manager.get_cursor() as cur:
            # Get profile count
            cur.execute("SELECT COUNT(*) as total FROM profiles")
            total_profiles = cur.fetchone()['total']
            
            # Get snapshot count
            cur.execute("SELECT COUNT(*) as total FROM profile_snapshots")
            total_snapshots = cur.fetchone()['total']
            
            # Get recent activity
            cur.execute("""
                SELECT COUNT(*) as recent 
                FROM profile_snapshots 
                WHERE snapshot_timestamp > NOW() - INTERVAL '1 day'
            """)
            recent_snapshots = cur.fetchone()['recent']
            
        return jsonify({
            'total_profiles': total_profiles,
            'total_snapshots': total_snapshots,
            'recent_snapshots_24h': recent_snapshots,
            'monitored_profiles': session.get('monitored_profiles', [])
        })
        
    except Exception as e:
        logging.error(f"Error getting status: {e}")
        return jsonify({'error': str(e)})

async def run_scraping(profiles):
    """Run the scraping process"""
    scraper = type('Scraper', (), {})()  # Simple container
    scraper.db_manager = db_manager
    scraper.scraply_service = scrapfly_service  
    scraper.incremental_logic = incremental_scraper
    
    results = []
    
    for username in profiles:
        try:
            logging.info(f"Scraping profile: {username}")
            profile_data = await scrapfly_service.scrape_profile(username)
            
            if not profile_data:
                results.append({
                    'username': username,
                    'status': 'failed',
                    'error': 'Failed to scrape profile'
                })
                continue
            
            profile_id = incremental_scraper.get_or_create_profile(username, profile_data)
            
            should_create, change_analysis = incremental_scraper.should_create_snapshot(
                profile_id, profile_data
            )
            
            result = {
                'username': username,
                'status': 'success',
                'profile_id': profile_id,
                'new_snapshot': should_create,
                'reason': change_analysis['reason']
            }
            
            if should_create:
                snapshot_id = incremental_scraper.create_snapshot(
                    profile_id, profile_data, change_analysis
                )
                result['snapshot_id'] = snapshot_id
            
            results.append(result)
            await asyncio.sleep(2)  # Rate limiting
            
        except Exception as e:
            logging.error(f"Failed to process {username}: {e}")
            results.append({
                'username': username,
                'status': 'error', 
                'error': str(e)
            })
    
    return results

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
