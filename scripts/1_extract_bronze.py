import requests
import time
import json
import os
import sys
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

#  Configuration (reads from .env / environment variables)
API_KEY = os.getenv("STEAM_API_KEY")
if not API_KEY:
    raise EnvironmentError("❌ STEAM_API_KEY not set. Check your .env file or environment variables.")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
STALE_HOURS = int(os.getenv("STALE_HOURS", "24"))  # Refresh games older than this

#  File paths
BRONZE_DIR     = "data/bronze"
REGISTRY_PATH  = "data/game_registry.json"

# ️ Network Armor
retry_strategy = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 AppData"
})


# ============================================================
#  Game Registry — Tracks what we've fetched and when
# ============================================================
def load_registry():
    if os.path.exists(REGISTRY_PATH):
        with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_registry(registry):
    os.makedirs(os.path.dirname(REGISTRY_PATH), exist_ok=True)
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)


def save_bronze(records, filepath):
    """Stream-write a list of dicts to JSON, one record per line, to avoid RAM spikes."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, item in enumerate(records):
            f.write(json.dumps(item, ensure_ascii=False))
            if i < len(records) - 1:
                f.write(",\n")
        f.write("\n]")


# ============================================================
#  Discovery — Find new games beyond the Charts list
# ============================================================
def discover_new_games(registry, limit=200):
    """
    Pull from Steam's full app list to discover games we've never seen.
    Returns a list of AppIDs (integers) not in our registry.
    """
    print("🔍 [DISCOVERY] Scanning Steam app catalog for new games...")
    try:
        # Using the official authenticated Steam API so it doesn't return Cloudflare 403 blocks
        url = f"https://api.steampowered.com/IStoreService/GetAppList/v1/?key={API_KEY}&max_results=50000"
        res = http.get(url, timeout=(10, 30)).json()
        all_apps = res.get("response", {}).get("apps", [])
        print(f"   📊 Steam catalog chunk: {len(all_apps):,} apps from official endpoint")
        
        # Filter: only apps we haven't seen, skip blank names
        known_ids = set(registry.keys())
        new_apps = [
            app['appid'] for app in all_apps
            if str(app['appid']) not in known_ids and app.get('name', '').strip()
        ]
        
        #  MAGIC TWEAK: Sort AppIDs descending. 
        # Steam assigns AppIDs sequentially. Higher AppID = Newer Game.
        # This forces the pipeline to always pull the newest releases instead of ancient random games!
        new_apps.sort(reverse=True)
        
        print(f"   🆕 Undiscovered apps (Newest First): {len(new_apps):,}")
        return new_apps[:limit]  # Return a batch for this run
    except Exception as e:
        print(f"   ⚠️ Discovery failed: {e}")
        return []


# ============================================================
#  Priority Queue — Decide which games to fetch this run
# ============================================================
def build_priority_queue(charts_ranks, registry, discovery_ids):
    """
    Build a priority-sorted list of AppIDs to fetch.
    
    Priority (highest → lowest):
      0. Charted games never fetched (new + popular)
      1. Charted games that are stale (popular refresh)
      2. Discovery games never fetched (new but unknown popularity)
    """
    now = datetime.utcnow()
    stale_cutoff = now - timedelta(hours=STALE_HOURS)
    
    candidates = []
    seen_appids = set()
    
    # --- Charts games (popular, always prioritized) ---
    for i, rank in enumerate(charts_ranks):
        appid = rank['appid']
        appid_str = str(appid)
        peak_players = rank.get('peak_in_game', 0)
        seen_appids.add(appid)
        
        reg_entry = registry.get(appid_str)
        
        if reg_entry is None:
            candidates.append({
                'appid': appid, 'rank': i, 'peak_players': peak_players,
                'priority': 0, 'source': 'charts_new',
            })
        else:
            try:
                last_updated = datetime.fromisoformat(reg_entry['last_updated'])
            except Exception:
                # If the registry date was manually edited/corrupted, assume it's extremely old
                last_updated = datetime.min
                
            if last_updated > stale_cutoff:
                continue  # Recently fetched — skip
            candidates.append({
                'appid': appid, 'rank': i, 'peak_players': peak_players,
                'priority': 1, 'source': 'charts_refresh',
            })
    
    # --- Discovery games (new games from full catalog) ---
    for j, appid in enumerate(discovery_ids):
        if appid in seen_appids:
            continue
        candidates.append({
            'appid': appid, 'rank': 1000 + j, 'peak_players': 0,
            'priority': 2, 'source': 'discovery',
        })
    
    # Sort: charts_new first, then charts_refresh, then discovery
    candidates.sort(key=lambda c: (c['priority'], c['rank']))
    
    return candidates


# ============================================================
#  Main Extraction
# ============================================================
def extract_steam_bronze_data():
    print("🚀 [TASK START] Extracting Bronze Data (Incremental)...")
    print(f"📊 [CONFIG] Batch size: {BATCH_SIZE}, Stale threshold: {STALE_HOURS}h")
    
    # --- Step 1: Get most-played games from Charts API ---
    charts_url = f"https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/?key={API_KEY}"
    try:
        all_ranks = http.get(charts_url, timeout=(10, 30)).json()['response']['ranks']
    except Exception as e:
        print(f"❌ [FATAL] Charts API Failed: {e}")
        sys.exit(1)

    if not all_ranks:
        print("❌ [FATAL] Charts API returned 0 games. Aborting.")
        sys.exit(1)
    print(f"📊 [CHARTS] {len(all_ranks)} most-played games.")

    # --- Step 2: Load registry ---
    registry = load_registry()
    print(f"📋 [REGISTRY] {len(registry)} games previously tracked.")
    
    # --- Step 3: Discover new games from the full Steam catalog ---
    discovery_ids = discover_new_games(registry, limit=BATCH_SIZE)
    
    # --- Step 4: Build priority queue ---
    queue = build_priority_queue(all_ranks, registry, discovery_ids)
    batch = queue[:BATCH_SIZE]
    
    if not batch:
        print("✅ [DONE] All charted games are up-to-date and no new discoveries. Nothing to do.")
        return
    
    sources = {}
    for g in batch:
        sources[g['source']] = sources.get(g['source'], 0) + 1
    source_str = ", ".join(f"{v} {k}" for k, v in sources.items())
    print(f"🎯 [QUEUE] Batch: {len(batch)} games ({source_str})")

    # --- Step 5: Fetch store + review data (batch-only, no legacy load) ---
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    store_records  = []  # Only this run's games — never loads the full archive
    review_records = []

    fetched = 0
    failed_ids = []

    for item in batch:
        appid = item['appid']
        print(f"📡 [{fetched+1}/{len(batch)}] AppID {appid} [{item['source']}]...", end=" ")
        
        store_url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc=us"
        reviews_url = f"https://store.steampowered.com/appreviews/{appid}?json=1&language=all&purchase_type=all&num_per_page=0"
        
        try:
            store_req = http.get(store_url, timeout=(10, 30))
            if store_req.status_code == 429:
                print("\n🚨 [RATE LIMIT] Exceeded Steam Store API rate limit! Backing off for 5 minutes...")
                time.sleep(300)
                continue
            
            store_res = store_req.json()
            time.sleep(1.5)  # Strict 1.5s pace for the Store API
            
            review_req = http.get(reviews_url, timeout=(10, 30))
            if review_req.status_code == 429:
                print("\n🚨 [RATE LIMIT] Exceeded Steam Reviews API rate limit! Backing off for 5 minutes...")
                time.sleep(300)
                continue
                
            review_res = review_req.json()
            time.sleep(0.5)  # 0.5s pace for Review API
            
            if store_res[str(appid)]['success']:
                game_data = store_res[str(appid)]['data']
                game_data['appid'] = appid
                game_data['live_peak_players'] = item['peak_players']

                store_records.append(game_data)

                if review_res.get('success') == 1:
                    summary = review_res.get('query_summary', {})
                else:
                    summary = {"total_positive": 0, "total_negative": 0}
                summary['appid'] = appid
                review_records.append(summary)

                registry[str(appid)] = {
                    "name": game_data.get('name', 'Unknown'),
                    "rank": item['rank'],
                    "peak_players": item['peak_players'],
                    "last_updated": datetime.utcnow().isoformat(),
                }

                fetched += 1
                print(f"✅ {game_data['name']}")
            else:
                print(f"⚠️ success=false")
                failed_ids.append(appid)
                # Mark as seen so we don't retry bad IDs every run
                registry[str(appid)] = {
                    "name": "FAILED",
                    "rank": item['rank'],
                    "peak_players": 0,
                    "last_updated": datetime.utcnow().isoformat(),
                }
                
        except Exception as e:
            print(f"⚠️ {e}")
            failed_ids.append(appid)

    # ============================================================
    #  Pre-Save Validation
    # ============================================================
    print("\n" + "=" * 60)
    print("🧪 [VALIDATION] Pre-Save Integrity Check")
    print("=" * 60)
    
    passed = True

    if fetched == 0:
        print("   ❌ No games fetched this run!")
        passed = False
    else:
        print(f"   ✅ Fetched: {fetched} games this run")

    if len(store_records) != len(review_records):
        print(f"   ❌ COUNT MISMATCH: {len(store_records)} store vs {len(review_records)} reviews")
        passed = False
    else:
        print(f"   ✅ Store/Reviews count match: {len(store_records)} records")

    store_ids  = {r['appid'] for r in store_records}
    review_ids = {r['appid'] for r in review_records}
    if store_ids != review_ids:
        missing = store_ids.symmetric_difference(review_ids)
        print(f"   ❌ AppID mismatch between store and reviews: {missing}")
        passed = False
    else:
        print(f"   ✅ AppIDs match between store and reviews")
    
    if failed_ids:
        print(f"   ⚠️ Failed AppIDs ({len(failed_ids)}): {failed_ids}")
    
    print("=" * 60)
    if not passed:
        print("🚨 [VALIDATION FAILED] Aborting save.")
        sys.exit(1)
    print("✅ [VALIDATION PASSED]\n")

    # --- Step 7: Save this batch to timestamped files ---
    store_path   = f"{BRONZE_DIR}/store_raw_{timestamp}.json"
    reviews_path = f"{BRONZE_DIR}/reviews_raw_{timestamp}.json"
    save_bronze(store_records,  store_path)
    save_bronze(review_records, reviews_path)
    save_registry(registry)

    print(f"🎉 [COMPLETE] Saved batch of {fetched} games → {store_path}")
    print(f"   📋 Registry: {len(registry)} games tracked total")

if __name__ == "__main__":
    extract_steam_bronze_data()