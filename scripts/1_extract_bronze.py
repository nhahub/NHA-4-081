import requests
import time
import json
import os
import sys
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 🔑 Configuration (reads from .env / environment variables)
API_KEY = os.getenv("STEAM_API_KEY")
if not API_KEY:
    raise EnvironmentError("❌ STEAM_API_KEY not set. Check your .env file or environment variables.")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
STALE_HOURS = int(os.getenv("STALE_HOURS", "24"))  # Refresh games older than this

# 📂 File paths
REGISTRY_PATH   = "data/game_registry.json"
STORE_PATH      = "data/bronze/store_raw.json"
REVIEWS_PATH    = "data/bronze/reviews_raw.json"

# 🛡️ Network Armor
retry_strategy = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)
http.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 AppData"
})


# ============================================================
# 📋 Game Registry — Tracks what we've fetched and when
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


# ============================================================
# 📂 Bronze Data — Load/merge existing data
# ============================================================
def load_existing_bronze(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {item['appid']: item for item in data if 'appid' in item}
    return {}

def save_bronze(data_dict, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(list(data_dict.values()), f, indent=4, ensure_ascii=False)


# ============================================================
# 🔍 Discovery — Find new games beyond the Charts list
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
        
        print(f"   🆕 Undiscovered apps: {len(new_apps):,}")
        return new_apps[:limit]  # Return a batch for this run
    except Exception as e:
        print(f"   ⚠️ Discovery failed: {e}")
        return []


# ============================================================
# 🎯 Priority Queue — Decide which games to fetch this run
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
# 🚀 Main Extraction
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

    # --- Step 5: Load existing bronze data for merging ---
    store_data = load_existing_bronze(STORE_PATH)
    reviews_data = load_existing_bronze(REVIEWS_PATH)
    print(f"📦 [BRONZE] Existing: {len(store_data)} store, {len(reviews_data)} reviews")

    # --- Step 6: Fetch store + review data ---
    fetched = 0
    failed_ids = []
    
    for item in batch:
        appid = item['appid']
        print(f"📡 [{fetched+1}/{len(batch)}] AppID {appid} [{item['source']}]...", end=" ")
        
        store_url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc=us"
        reviews_url = f"https://store.steampowered.com/appreviews/{appid}?json=1&language=all&purchase_type=all&num_per_page=0"
        
        try:
            store_res = http.get(store_url, timeout=(10, 30)).json()
            time.sleep(1.0)
            
            review_res = http.get(reviews_url, timeout=(10, 30)).json()
            time.sleep(1.0)
            
            if store_res[str(appid)]['success']:
                game_data = store_res[str(appid)]['data']
                game_data['appid'] = appid
                game_data['live_peak_players'] = item['peak_players']
                
                store_data[appid] = game_data
                
                if review_res.get('success') == 1:
                    summary = review_res.get('query_summary', {})
                else:
                    summary = {"total_positive": 0, "total_negative": 0}
                summary['appid'] = appid
                reviews_data[appid] = summary
                
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
    # 🧪 Pre-Save Validation
    # ============================================================
    print("\n" + "=" * 60)
    print("🧪 [VALIDATION] Pre-Save Integrity Check")
    print("=" * 60)
    
    passed = True
    
    if fetched == 0 and len(store_data) > 0:
        print(f"   ⚠️ No NEW games fetched, but {len(store_data)} existing games preserved.")
    elif fetched == 0:
        print("   ❌ No games fetched and no existing data!")
        passed = False
    else:
        print(f"   ✅ Fetched: {fetched} games this run")
    
    if len(store_data) != len(reviews_data):
        print(f"   ❌ COUNT MISMATCH: {len(store_data)} store vs {len(reviews_data)} reviews")
        passed = False
    else:
        print(f"   ✅ Store/Reviews count match: {len(store_data)} total")
    
    store_ids = set(store_data.keys())
    review_ids = set(reviews_data.keys())
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

    # --- Step 7: Save everything ---
    save_bronze(store_data, STORE_PATH)
    save_bronze(reviews_data, REVIEWS_PATH)
    save_registry(registry)
    
    print(f"🎉 [COMPLETE] Bronze: {len(store_data)} total games ({fetched} new/refreshed)")
    print(f"   📋 Registry: {len(registry)} games tracked")

if __name__ == "__main__":
    extract_steam_bronze_data()