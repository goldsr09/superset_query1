from flask import Flask, render_template, request, jsonify
import requests
import json
import uuid
import threading
import sqlite3
from datetime import datetime, timedelta

app = Flask(__name__)

# --- Superset API Config ---
SUPERSET_EXECUTE_URL = "https://superset.de.gcp.rokulabs.net/api/v1/sqllab/execute/"
SUPERSET_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Referer": "https://superset.de.gcp.rokulabs.net/sqllab/",
    "Origin": "https://superset.de.gcp.rokulabs.net",
    "X-CSRFToken": "IjY5NTkwYWQ5NDZkZGU4ZjFmMDE4YWM1OTAzMWMxOGVjMmVhN2EyZGMi.aI_eMg.wqTQAgFNco6h8bC7E8UTpYs4SKc",
    "Cookie": "session=.eJxVjk1LAzEUAP9LzhXykpe8pDehokIvSkE8Ldn3QUvLLuy2ooj_3YAnrwMzzLcbbNH16LbX5aYbN5zEbR1XkMIjcVL2FbAa-1TQhESjegLvE4ZYGBijYSMsDbHGKC2YVuTgx1pUyUfvx0LNMEq2REAgqYwsBJaz-aaUQayOMYBQbMDZ2KrrI7dVl78bQAqd8LrYcJ3POnWWa6q-ScUsosXAPJTGnUVgKMpBG7Ug3L3LzO2i3enixs0n4eFf6nx9_WzDPj5_3I14__4QDl9PaXd4Oe6nKcDjm_v5BWS1Vgo.aI_dyQ.vaP1EMwptCPu88FaiKByWt03Gdo"
}
SUPERSET_DB_ID = 2

# --- Query Template ---
QUERY_TEMPLATE = """
SELECT 
    f.date_key,
    CAST(d AS VARCHAR) AS deal_id,
    COALESCE(h.name, CAST(d AS VARCHAR)) AS deal_name,
    SUM(f.is_impressed) AS imps,
    SUM(CASE WHEN f.ad_fetch_source IS NULL AND f.event_type = 'candidate' THEN 1 ELSE 0 END) AS real_demand_returned,
    SUM(f.num_opportunities) AS oppos,
    SUM(f.is_selected) AS total_bids,
    SUM(CASE WHEN f.is_impressed IS NULL AND f.event_type = 'candidate' AND f.ad_fetch_source IS NULL THEN 1 ELSE 0 END) AS Loss_Totals,
    (SUM(f.is_impressed) * 1.000 / NULLIF(SUM(f.num_opportunities), 0)) * 100.00 AS fill_rate,
    SUM(CASE WHEN f.event_type = 'demand_request' THEN 1 ELSE 0 END) AS Requests,
    (SUM(f.is_impressed) * 1.000 / NULLIF(SUM(f.is_selected), 0)) * 100.00 AS render_rate,
    (SUM(f.is_selected) * 1.000 / NULLIF(SUM(CASE WHEN f.ad_fetch_source IS NULL AND f.event_type = 'candidate' THEN 1 ELSE 0 END), 0)) * 100.00 AS win_rate,
    SUM(CASE WHEN f.event_type = 'candidate' THEN 1 ELSE 0 END) AS candidates
FROM
    advertising.demand_funnel f
CROSS JOIN UNNEST(f.deal_id) AS t(d)
LEFT JOIN ads.dim_rams_deals_history h
    ON CAST(d AS VARCHAR) = CAST(h.id AS VARCHAR)
    AND f.date_key = h.date_key
WHERE 
    f.date_key >= '{date_from}'
    {date_to_condition}
    AND CONTAINS(f.demand_systems, 'PARTNER_AD_SERVER_VIDEO')
    {deal_name_condition}
GROUP BY 
    f.date_key, CAST(d AS VARCHAR), COALESCE(h.name, CAST(d AS VARCHAR))
ORDER BY 
    f.date_key ASC, deal_name ASC
"""
    f.date_key, CAST(d AS VARCHAR), h.name
ORDER BY 
    f.date_key ASC, CAST(d AS VARCHAR) ASC
LIMIT 10000
"""

# --- In-Memory Job Tracking ---
JOBS = {}

# --- SQLite Cache ---
DB_PATH = "query_cache.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Check if table exists and has the new normalized structure
    c.execute("PRAGMA table_info(query_results)")
    columns = [row[1] for row in c.fetchall()]
    
    # If old schema or missing columns, recreate table
    if 'deal_name' not in columns or 'imps' not in columns:
        c.execute("DROP TABLE IF EXISTS query_results")
        print("Recreated database schema for normalized deal storage")
    
    c.execute("""
    CREATE TABLE IF NOT EXISTS query_results (
        date_key TEXT,
        deal_id TEXT,
        deal_name TEXT,
        imps REAL,
        oppos REAL,
        total_bids REAL,
        fill_rate REAL,
        render_rate REAL,
        requests REAL,
        original_data JSON,
        created_at TIMESTAMP,
        PRIMARY KEY (deal_name, date_key)
    )
    """)
    
    # Create indexes for better query performance
    c.execute("CREATE INDEX IF NOT EXISTS idx_deal_name ON query_results(deal_name)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_date_key ON query_results(date_key)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_deal_date ON query_results(deal_name, date_key)")
    
    conn.commit()
    conn.close()

init_db()

def save_results_to_db(job_id, deal_names, date_from, date_to, rows):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    inserted_count = 0
    duplicate_count = 0
    
    # Insert each row individually with extracted fields for better querying
    for row in rows:
        date_key = str(row.get("date_key"))
        deal_name = str(row.get("deal_name", ""))
        deal_id = str(row.get("deal_id", ""))
        
        # Only insert if we have valid deal information
        if deal_name and deal_name.strip():
            try:
                c.execute("""
                    INSERT INTO query_results(
                        date_key, deal_id, deal_name, imps, oppos, total_bids, 
                        fill_rate, render_rate, requests, original_data, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    date_key,
                    deal_id,
                    deal_name.strip(),
                    float(row.get('imps', 0) or 0),
                    float(row.get('oppos', 0) or 0),
                    float(row.get('total_bids', 0) or 0),
                    float(row.get('fill_rate', 0) or 0),
                    float(row.get('render_rate', 0) or 0),
                    float(row.get('Requests', 0) or 0),
                    json.dumps(row),
                    datetime.now()
                ))
                inserted_count += 1
            except sqlite3.IntegrityError:
                # This combination already exists, skip it
                duplicate_count += 1
                continue
    
    conn.commit()
    conn.close()
    
    print(f"[CACHE] Inserted {inserted_count} new rows, skipped {duplicate_count} duplicates")
    return inserted_count

def get_cached_results(deal_names, date_from, date_to):
    """Get cached results for multiple deal names with smart incremental querying"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # For multiple deal names, we need to query each one and combine results
    all_cached_rows = []
    missing_ranges = []
    
    for deal_name in deal_names:
        # Check what date range we have cached for this deal
        c.execute("""
            SELECT MIN(date_key) as cached_min, MAX(date_key) as cached_max, COUNT(*) as cached_count
            FROM query_results 
            WHERE deal_name LIKE ?
        """, (f'%{deal_name}%',))
        
        cache_info = c.fetchone()
        cached_min, cached_max, cached_count = cache_info if cache_info else (None, None, 0)
        
        if cached_count == 0:
            print(f"[CACHE] Deal '{deal_name}': No cached data found - need full query")
            missing_ranges.append({
                'deal_name': deal_name,
                'date_from': date_from,
                'date_to': date_to,
                'reason': 'no_cache'
            })
            continue    
        
        print(f"[DEBUG] Cache coverage check for '{deal_name}':")
        print(f"[DEBUG] - Cached range: {cached_min} to {cached_max}")
        print(f"[DEBUG] - Requested range: {date_from} to {date_to}")
        
        # Check for different overlap scenarios
        cache_covers_request = (cached_min <= date_from and cached_max >= date_to)
        
        if cache_covers_request:
            # Full coverage - use cache completely
            print(f"[DEBUG] - Full coverage available")
            
            # Get the requested data from cache
            c.execute("""
                SELECT date_key, deal_id, deal_name, imps, oppos, total_bids, 
                       fill_rate, render_rate, requests, original_data
                FROM query_results 
                WHERE deal_name LIKE ? AND date_key >= ? AND date_key <= ?
                ORDER BY date_key ASC
            """, (f'%{deal_name}%', date_from, date_to))
            
            rows = c.fetchall()
            
            # Check if we have continuous coverage (no gaps in dates)
            if rows:
                date_set = set(row[0] for row in rows)
                current_date = datetime.strptime(date_from, '%Y-%m-%d')
                end_date = datetime.strptime(date_to, '%Y-%m-%d')
                
                has_full_coverage = True
                missing_dates = []
                while current_date <= end_date:
                    date_str = current_date.strftime('%Y-%m-%d')
                    if date_str not in date_set:
                        has_full_coverage = False
                        missing_dates.append(date_str)
                    current_date += timedelta(days=1)
                
                if has_full_coverage:
                    print(f"[CACHE] Deal '{deal_name}': Using full cache ({len(rows)} days)")
                    
                    # Convert to the expected format using original_data
                    for row in rows:
                        # Parse the original JSON data to get all fields
                        try:
                            original_data = json.loads(row[9]) if row[9] else {}
                        except:
                            original_data = {}
                        
                        # Use original data if available, otherwise fall back to cached columns
                        all_cached_rows.append({
                            'date_key': row[0],
                            'deal_id': row[1], 
                            'deal_name': row[2],
                            'imps': float(original_data.get('imps', row[3]) or 0),
                            'oppos': float(original_data.get('oppos', row[4]) or 0),
                            'total_bids': float(original_data.get('total_bids', row[5]) or 0),
                            'fill_rate': float(original_data.get('fill_rate', row[6]) or 0),
                            'render_rate': float(original_data.get('render_rate', row[7]) or 0),
                            'Requests': float(original_data.get('Requests', row[8]) or 0),
                            'real_demand_returned': float(original_data.get('real_demand_returned', 0) or 0),
                            'Loss_Totals': float(original_data.get('Loss_Totals', 0) or 0),
                            'win_rate': float(original_data.get('win_rate', 0) or 0),
                            'candidates': float(original_data.get('candidates', 0) or 0)
                        })
                else:
                    print(f"[CACHE] Deal '{deal_name}': Missing {len(missing_dates)} dates in cached range")
                    # Could implement gap-filling here, but for now treat as missing
                    missing_ranges.append({
                        'deal_name': deal_name,
                        'date_from': date_from,
                        'date_to': date_to,
                        'reason': 'gaps_in_cache'
                    })
            else:
                print(f"[CACHE] Deal '{deal_name}': No data found in requested range")
                missing_ranges.append({
                    'deal_name': deal_name,
                    'date_from': date_from,
                    'date_to': date_to,
                    'reason': 'no_data_in_range'
                })
                
        else:
            # Partial coverage - check for incremental query opportunities
            cache_overlap = not (cached_max < date_from or cached_min > date_to)
            
            if cache_overlap:
                print(f"[DEBUG] - Partial coverage detected")
                
                # Get overlapping cached data
                overlap_start = max(date_from, cached_min)
                overlap_end = min(date_to, cached_max)
                
                c.execute("""
                    SELECT date_key, deal_id, deal_name, imps, oppos, total_bids, 
                           fill_rate, render_rate, requests, original_data
                    FROM query_results 
                    WHERE deal_name LIKE ? AND date_key >= ? AND date_key <= ?
                    ORDER BY date_key ASC
                """, (f'%{deal_name}%', overlap_start, overlap_end))
                
                cached_rows = c.fetchall()
                overlap_days = len(cached_rows)
                requested_days = (datetime.strptime(date_to, '%Y-%m-%d') - datetime.strptime(date_from, '%Y-%m-%d')).days + 1
                coverage_percent = (overlap_days / requested_days) * 100
                
                print(f"[DEBUG] - Overlap coverage: {coverage_percent:.1f}% ({overlap_days}/{requested_days} days)")
                
                # If we have significant overlap (>50%), use incremental approach
                if coverage_percent >= 50:
                    print(f"[CACHE] Deal '{deal_name}': Using incremental query approach")
                    
                    # Add cached data to results using original_data
                    for row in cached_rows:
                        # Parse the original JSON data to get all fields
                        try:
                            original_data = json.loads(row[9]) if row[9] else {}
                        except:
                            original_data = {}
                        
                        # Use original data if available, otherwise fall back to cached columns
                        all_cached_rows.append({
                            'date_key': row[0],
                            'deal_id': row[1], 
                            'deal_name': row[2],
                            'imps': float(original_data.get('imps', row[3]) or 0),
                            'oppos': float(original_data.get('oppos', row[4]) or 0),
                            'total_bids': float(original_data.get('total_bids', row[5]) or 0),
                            'fill_rate': float(original_data.get('fill_rate', row[6]) or 0),
                            'render_rate': float(original_data.get('render_rate', row[7]) or 0),
                            'Requests': float(original_data.get('Requests', row[8]) or 0),
                            'real_demand_returned': float(original_data.get('real_demand_returned', 0) or 0),
                            'Loss_Totals': float(original_data.get('Loss_Totals', 0) or 0),
                            'win_rate': float(original_data.get('win_rate', 0) or 0),
                            'candidates': float(original_data.get('candidates', 0) or 0)
                        })
                    
                    # Determine missing date ranges to query
                    if date_from < cached_min:
                        # Need data before cached range
                        end_before = (datetime.strptime(cached_min, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
                        missing_ranges.append({
                            'deal_name': deal_name,
                            'date_from': date_from,
                            'date_to': end_before,
                            'reason': 'before_cache'
                        })
                    
                    if date_to > cached_max:
                        # Need data after cached range
                        start_after = (datetime.strptime(cached_max, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
                        missing_ranges.append({
                            'deal_name': deal_name,
                            'date_from': start_after,
                            'date_to': date_to,
                            'reason': 'after_cache'
                        })
                else:
                    print(f"[CACHE] Deal '{deal_name}': Coverage too low for incremental approach")
                    missing_ranges.append({
                        'deal_name': deal_name,
                        'date_from': date_from,
                        'date_to': date_to,
                        'reason': 'low_coverage'
                    })
            else:
                print(f"[DEBUG] - No overlap between cache and requested range")
                missing_ranges.append({
                    'deal_name': deal_name,
                    'date_from': date_from,
                    'date_to': date_to,
                    'reason': 'no_overlap'
                })
    
    conn.close()
    
    # Return results: cached data and list of ranges that need fresh queries
    return all_cached_rows, missing_ranges

def run_incremental_superset_query(job_id, missing_ranges, original_date_from, original_date_to):
    """Run multiple targeted queries for missing date ranges and combine with cached data"""
    print(f"[JOB {job_id}] Running incremental queries for {len(missing_ranges)} missing ranges...")
    
    all_new_rows = []
    cached_rows = JOBS[job_id].get("cached_rows", [])
    
    for i, range_info in enumerate(missing_ranges):
        deal_name = range_info['deal_name']
        date_from = range_info['date_from']
        date_to = range_info['date_to']
        reason = range_info['reason']
        
        print(f"[JOB {job_id}] Query {i+1}/{len(missing_ranges)}: {deal_name} from {date_from} to {date_to} ({reason})")
        
        # Build targeted SQL query for this missing range
        date_to_condition = f"AND f.date_key <= '{date_to}'" if date_to != date_from else ""
        deal_name_condition = f"AND h.name LIKE '%{deal_name}%'"
        
        sql_query = QUERY_TEMPLATE.format(
            date_from=date_from,
            date_to_condition=date_to_condition,
            deal_name_condition=deal_name_condition
        )
        
        print(f"[DEBUG] Incremental query {i+1}: {sql_query[:200]}...")
        
        payload = {
            "database_id": SUPERSET_DB_ID,
            "schema": "advertising",
            "sql": sql_query,
            "runAsync": False
        }
        
        try:
            resp = requests.post(
                SUPERSET_EXECUTE_URL,
                headers=SUPERSET_HEADERS,
                data=json.dumps(payload),
                timeout=300  # 5 minutes per query
            )
            
            if resp.status_code == 200:
                resp_data = resp.json()
                rows = resp_data.get("data", [])
                print(f"[JOB {job_id}] Query {i+1} completed: {len(rows)} new rows")
                all_new_rows.extend(rows)
                
                # Save incremental results to cache
                save_results_to_db(job_id, [deal_name], date_from, date_to, rows)
            else:
                print(f"[JOB {job_id}] Query {i+1} failed: {resp.text}")
                JOBS[job_id] = {"status": "error", "message": f"Incremental query failed: {resp.text}"}
                return
                
        except Exception as e:
            print(f"[JOB {job_id}] Query {i+1} exception: {e}")
            JOBS[job_id] = {"status": "error", "message": f"Incremental query exception: {e}"}
            return
        
        # Update progress
        progress = ((i + 1) / len(missing_ranges)) * 100
        JOBS[job_id]["progress"] = progress
    
    # Combine cached data with new data
    combined_rows = cached_rows + all_new_rows
    
    # Sort by date for consistent ordering
    combined_rows.sort(key=lambda x: (x.get('date_key', ''), x.get('deal_name', '')))
    
    # Determine columns safely
    if combined_rows:
        columns = list(combined_rows[0].keys())
    else:
        columns = []
    
    JOBS[job_id] = {
        "status": "success",
        "columns": columns,
        "rows": combined_rows,
        "progress": 100,
        "incremental_summary": {
            "cached_rows": len(cached_rows),
            "new_rows": len(all_new_rows),
            "total_rows": len(combined_rows),
            "queries_executed": len(missing_ranges)
        }
    }
    
    print(f"[JOB {job_id}] Incremental query completed: {len(cached_rows)} cached + {len(all_new_rows)} new = {len(combined_rows)} total rows")

# --- Background Job ---
# --- Background Job ---
def run_superset_query(job_id, deal_names, date_from, date_to, sql_query):
    JOBS[job_id] = {"status": "running", "progress": 0}
    print(f"[JOB {job_id}] Running query for deals '{', '.join(deal_names)}' from {date_from} to {date_to}...")

    payload = {
        "database_id": SUPERSET_DB_ID,
        "schema": "advertising",
        "sql": sql_query,
        "runAsync": False
    }

    try:
        resp = requests.post(
            SUPERSET_EXECUTE_URL,
            headers=SUPERSET_HEADERS,
            data=json.dumps(payload),
            timeout=600  # 10 minutes
        )
    except requests.exceptions.Timeout:
        JOBS[job_id] = {"status": "error", "message": "Query timed out after 10 minutes"}
        return

    if resp.status_code != 200:
        JOBS[job_id] = {"status": "error", "message": f"Superset HTTP error: {resp.text}"}
        return

    try:
        resp_data = resp.json()
        rows = resp_data.get("data", [])

        # --- Determine columns safely ---
        if "columns" in resp_data and resp_data["columns"]:
            columns = [col.get("name") or col.get("column_name") for col in resp_data["columns"]]
        elif rows:
            columns = list(rows[0].keys())  # fallback if columns missing
        else:
            columns = []

        # Save to cache
        save_results_to_db(job_id, deal_names, date_from, date_to, rows)

        JOBS[job_id] = {
            "status": "success",
            "columns": columns,
            "rows": rows,
            "progress": 100
        }
        print(f"[JOB {job_id}] Completed successfully. {len(rows)} rows cached.")
    except Exception as e:
        JOBS[job_id] = {
            "status": "error",
            "message": f"Failed to parse Superset JSON: {e}\nRaw: {resp.text[:500]}"
        }
        print(f"[JOB {job_id}] JSON parse failed: {e}")

# --- Flask Routes ---
@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/dod-graphs", methods=["GET"])
def dod_graphs():
    return render_template("dod_graphs.html")

@app.route("/breakout-analysis", methods=["GET"])
def breakout_analysis():
    return render_template("breakout_analysis.html")

@app.route("/run_query", methods=["POST"])
def run_query():
    # Get multiple deal names
    deal_names = request.form.getlist("deal_name")
    deal_names = [name.strip() for name in deal_names if name.strip()]
    
    # Get date range parameters
    date_from = request.form.get("date_from", "2025-01-07").strip()
    date_to = request.form.get("date_to", "").strip()
    
    if not deal_names:
        return jsonify({"error": "Please enter at least one deal name!"}), 400
    
    # Set default date_to if not provided
    if not date_to:
        date_to = date_from

    # Check cache first
    cached_rows, missing_ranges = get_cached_results(deal_names, date_from, date_to)
    
    if not missing_ranges:
        # Full cache hit - return cached data
        job_id = str(uuid.uuid4())
        JOBS[job_id] = {
            "status": "success",
            "columns": list(cached_rows[0].keys()) if cached_rows else [],
            "rows": cached_rows,
            "progress": 100
        }
        print(f"[JOB {job_id}] Returned from cache with {len(cached_rows)} rows.")
        return jsonify({"job_id": job_id, "cached": True})
    
    elif cached_rows and missing_ranges:
        # Partial cache hit - use incremental query approach
        print(f"[CACHE] Using incremental query: {len(cached_rows)} cached rows, {len(missing_ranges)} missing ranges")
        
        # Launch background job for missing ranges only
        job_id = str(uuid.uuid4())
        
        # Store cached data in job for later combination
        JOBS[job_id] = {
            "status": "running", 
            "progress": 0,
            "cached_rows": cached_rows,
            "missing_ranges": missing_ranges
        }
        
        threading.Thread(target=run_incremental_superset_query, args=(job_id, missing_ranges, date_from, date_to), daemon=True).start()
        return jsonify({"job_id": job_id, "cached": False, "incremental": True})
    
    else:
        # No cache hit - query everything
        print(f"[CACHE] No cache available, querying full range")
        
        # Build SQL query for full range
        date_to_condition = f"AND f.date_key <= '{date_to}'" if date_to and date_to != date_from else ""
        
        # Build deal name condition for multiple names
        if len(deal_names) == 1:
            deal_name_condition = f"AND h.name LIKE '%{deal_names[0]}%'"
        else:
            like_conditions = [f"h.name LIKE '%{name}%'" for name in deal_names]
            deal_name_condition = f"AND ({' OR '.join(like_conditions)})"
        
        sql_query = QUERY_TEMPLATE.format(
            date_from=date_from,
            date_to_condition=date_to_condition,
            deal_name_condition=deal_name_condition
        )

        print(f"[DEBUG] Generated SQL query:")
        print(f"[DEBUG] {sql_query}")

        # Launch background job
        job_id = str(uuid.uuid4())
        threading.Thread(target=run_superset_query, args=(job_id, deal_names, date_from, date_to, sql_query), daemon=True).start()

        return jsonify({"job_id": job_id, "cached": False})

@app.route("/dod_data", methods=["POST"])
def get_dod_data():
    # Get parameters
    deal_names = request.form.getlist("deal_name")
    deal_names = [name.strip() for name in deal_names if name.strip()]
    date_from = request.form.get("date_from", "2025-07-01").strip()
    date_to = request.form.get("date_to", "2025-08-03").strip()
    metric = request.form.get("metric", "imps").strip()
    dual_metric = request.form.get("dual_metric", "").strip()  # New parameter for dual metrics
    
    if not deal_names:
        return jsonify({"status": "error", "message": "Please enter at least one deal name!"}), 400
    
    # For DoD graphs, we should only handle ONE deal at a time
    if len(deal_names) > 1:
        return jsonify({
            "status": "error", 
            "message": f"DoD graphs work best with a single deal. You provided {len(deal_names)} deals: {', '.join(deal_names)}. Please select only one deal for meaningful trend analysis."
        }), 400
    
    single_deal = deal_names[0]
    
    # Try to get data from cache first - but we need to find the right cached query
    # The cache might have this deal as part of a multi-deal query
    cached_rows = get_cached_results_for_single_deal(single_deal, date_from, date_to)
    
    if cached_rows:
        print(f"[DoD] Using cached data for single deal '{single_deal}': {len(cached_rows)} rows")
        # Aggregate cached data by date for DoD calculations
        from collections import defaultdict
        
        daily_aggregates = defaultdict(lambda: {
            'imps': 0, 'oppos': 0, 'total_bids': 0, 'requests': 0
        })
        
        # Group by date_key and sum metrics for this specific deal
        # No need for duplicate detection since we now have clean normalized data
        for row in cached_rows:
            date_key = row.get('date_key', '')
            
            if date_key:
                # Use the correct field names from cached data
                daily_aggregates[date_key]['imps'] += float(row.get('imps', 0) or 0)
                daily_aggregates[date_key]['oppos'] += float(row.get('oppos', 0) or 0)
                daily_aggregates[date_key]['total_bids'] += float(row.get('total_bids', 0) or 0)
                daily_aggregates[date_key]['requests'] += float(row.get('Requests', 0) or 0)
        
        # Calculate rates and sort by date
        aggregated_rows = []
        for date_key in sorted(daily_aggregates.keys()):
            agg = daily_aggregates[date_key]
            # Skip days with no data for this deal
            if agg['imps'] == 0 and agg['oppos'] == 0 and agg['total_bids'] == 0:
                continue
                
            fill_rate = (agg['imps'] / agg['oppos'] * 100) if agg['oppos'] > 0 else 0
            render_rate = (agg['imps'] / agg['total_bids'] * 100) if agg['total_bids'] > 0 else 0
            
            aggregated_rows.append({
                'date_key': date_key,
                'imps': agg['imps'],
                'oppos': agg['oppos'], 
                'total_bids': agg['total_bids'],
                'fill_rate': fill_rate,
                'render_rate': render_rate,
                'requests': agg['requests']
            })
        
        # Calculate DoD changes from cached data
        dod_data = []
        for i, row in enumerate(aggregated_rows):
            if i == 0:
                # First day - no previous day to compare
                dod_entry = {
                    "date_key": row["date_key"],
                    "value": row[metric] or 0,
                    "dod_change": 0,
                    "dod_percent": 0
                }
                # Add dual metric if specified
                if dual_metric and dual_metric in row:
                    dod_entry["dual_value"] = row[dual_metric] or 0
                    dod_entry["dual_dod_change"] = 0
                    dod_entry["dual_dod_percent"] = 0
                dod_data.append(dod_entry)
            else:
                current_value = row[metric] or 0
                previous_value = aggregated_rows[i-1][metric] or 0
                
                dod_change = current_value - previous_value
                dod_percent = (dod_change / previous_value * 100) if previous_value != 0 else 0
                
                dod_entry = {
                    "date_key": row["date_key"],
                    "value": current_value,
                    "dod_change": dod_change,
                    "dod_percent": dod_percent
                }
                
                # Add dual metric calculations if specified
                if dual_metric and dual_metric in row:
                    dual_current_value = row[dual_metric] or 0
                    dual_previous_value = aggregated_rows[i-1][dual_metric] or 0
                    
                    dual_dod_change = dual_current_value - dual_previous_value
                    dual_dod_percent = (dual_dod_change / dual_previous_value * 100) if dual_previous_value != 0 else 0
                    
                    dod_entry["dual_value"] = dual_current_value
                    dod_entry["dual_dod_change"] = dual_dod_change
                    dod_entry["dual_dod_percent"] = dual_dod_percent
                
                dod_data.append(dod_entry)
        
        return jsonify({
            "status": "success",
            "data": dod_data,
            "metric": metric,
            "dual_metric": dual_metric if dual_metric else None,
            "deal_names": [single_deal],
            "cached": True
        })
    
    else:
        # No cache - need to query Superset for single deal
        print(f"[DoD] No cache found, querying Superset for single deal '{single_deal}', {date_from} to {date_to}")
        
        # SQL for day-over-day data for single deal
        dod_sql = f"""
        SELECT 
            f.date_key,
            SUM(f.is_impressed) AS imps,
            SUM(f.num_opportunities) AS oppos,
            SUM(f.is_selected) AS total_bids,
            (SUM(f.is_impressed) * 1.000 / NULLIF(SUM(f.num_opportunities), 0)) * 100.00 AS fill_rate,
            (SUM(f.is_impressed) * 1.000 / NULLIF(SUM(f.is_selected), 0)) * 100.00 AS render_rate,
            SUM(CASE WHEN f.event_type = 'demand_request' THEN 1 ELSE 0 END) AS requests
        FROM
            advertising.demand_funnel f
        CROSS JOIN UNNEST(f.deal_id) AS t(d)
        LEFT JOIN ads.dim_rams_deals_history h
            ON CAST(d AS VARCHAR) = CAST(h.id AS VARCHAR)
            AND f.date_key = h.date_key
        WHERE 
            f.date_key >= '{date_from}'
            AND f.date_key <= '{date_to}'
            AND CONTAINS(f.demand_systems, 'PARTNER_AD_SERVER_VIDEO')
            AND h.name = '{single_deal}'
        GROUP BY 
            f.date_key
        ORDER BY 
            f.date_key ASC
        """
        
        payload = {
            "database_id": SUPERSET_DB_ID,
            "schema": "advertising",
            "sql": dod_sql,
            "runAsync": False
        }
        
        try:
            resp = requests.post(
                SUPERSET_EXECUTE_URL,
                headers=SUPERSET_HEADERS,
                data=json.dumps(payload),
                timeout=300
            )
            
            if resp.status_code == 200:
                data = resp.json()
                rows = data.get("data", [])
                
                # Calculate DoD changes
                dod_data = []
                for i, row in enumerate(rows):
                    if i == 0:
                        # First day - no previous day to compare
                        dod_entry = {
                            "date_key": row["date_key"],
                            "value": row[metric] or 0,
                            "dod_change": 0,
                            "dod_percent": 0
                        }
                        # Add dual metric if specified
                        if dual_metric and dual_metric in row:
                            dod_entry["dual_value"] = row[dual_metric] or 0
                            dod_entry["dual_dod_change"] = 0
                            dod_entry["dual_dod_percent"] = 0
                        dod_data.append(dod_entry)
                    else:
                        current_value = row[metric] or 0
                        previous_value = rows[i-1][metric] or 0
                        
                        dod_change = current_value - previous_value
                        dod_percent = (dod_change / previous_value * 100) if previous_value != 0 else 0
                        
                        dod_entry = {
                            "date_key": row["date_key"],
                            "value": current_value,
                            "dod_change": dod_change,
                            "dod_percent": dod_percent
                        }
                        
                        # Add dual metric calculations if specified
                        if dual_metric and dual_metric in row:
                            dual_current_value = row[dual_metric] or 0
                            dual_previous_value = rows[i-1][dual_metric] or 0
                            
                            dual_dod_change = dual_current_value - dual_previous_value
                            dual_dod_percent = (dual_dod_change / dual_previous_value * 100) if dual_previous_value != 0 else 0
                            
                            dod_entry["dual_value"] = dual_current_value
                            dod_entry["dual_dod_change"] = dual_dod_change
                            dod_entry["dual_dod_percent"] = dual_dod_percent
                        
                        dod_data.append(dod_entry)
                
                return jsonify({
                    "status": "success",
                    "data": dod_data,
                    "metric": metric,
                    "dual_metric": dual_metric if dual_metric else None,
                    "deal_names": [single_deal],
                    "cached": False
                })
            else:
                return jsonify({"status": "error", "message": resp.text})
                
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)})

def get_cached_results_for_single_deal(deal_name, date_from, date_to):
    """Get cached results for a specific deal within the date range"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Query the normalized table directly for this deal and date range
        cursor.execute("""
            SELECT date_key, deal_id, deal_name, imps, oppos, total_bids, 
                   fill_rate, render_rate, requests, original_data
            FROM query_results 
            WHERE deal_name = ? AND date_key >= ? AND date_key <= ?
            ORDER BY date_key ASC
        """, (deal_name, date_from, date_to))
        
        rows = cursor.fetchall()
        conn.close()
        
        if rows:
            # Convert to the expected format
            filtered_data = []
            for row in rows:
                filtered_data.append({
                    'date_key': row[0],
                    'deal_id': row[1], 
                    'deal_name': row[2],
                    'imps': float(row[3] or 0),
                    'oppos': float(row[4] or 0),
                    'total_bids': float(row[5] or 0),
                    'fill_rate': float(row[6] or 0),
                    'render_rate': float(row[7] or 0),
                    'Requests': float(row[8] or 0)  # Note: capital R for compatibility
                })
            
            print(f"[DoD] Found {len(filtered_data)} cached rows for deal '{deal_name}' (normalized)")
            return filtered_data
                
        return None
    except Exception as e:
        print(f"Error getting cached results for single deal: {e}")
        return None

@app.route("/get_available_deals", methods=["GET"])
def get_available_deals():
    """Get list of unique deal names from cached data"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get deal summary from normalized table
        cursor.execute("""
            SELECT 
                deal_name,
                MIN(date_key) as min_date,
                MAX(date_key) as max_date,
                COUNT(*) as row_count
            FROM query_results 
            WHERE deal_name IS NOT NULL AND deal_name != ''
            GROUP BY deal_name
            ORDER BY deal_name
        """)
        
        deal_summaries = cursor.fetchall()
        conn.close()
        
        # Format the response
        available_deals = []
        for deal_name, min_date, max_date, row_count in deal_summaries:
            # Format date range
            if min_date == max_date:
                date_range = min_date
            else:
                date_range = f"{min_date} to {max_date}"
            
            available_deals.append({
                "name": deal_name,
                "display_name": f"{deal_name} ({date_range}, {row_count} days)",
                "date_range": date_range,
                "row_count": row_count
            })
        
        return jsonify({
            "status": "success",
            "deals": available_deals,
            "total_count": len(available_deals)
        })
        
    except Exception as e:
        return jsonify({
            "status": "error", 
            "message": f"Failed to get available deals: {str(e)}"
        })

@app.route("/breakout_data", methods=["POST"])
def get_breakout_data():
    """Get fill rate data broken down by provider type from cached data"""
    # Get parameters
    date_from = request.form.get("date_from", "2025-07-01").strip()
    date_to = request.form.get("date_to", "2025-08-03").strip()
    
    try:
        # Get all cached data for the date range
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT date_key, deal_name, imps, oppos, total_bids, fill_rate, render_rate, requests
            FROM query_results 
            WHERE date_key >= ? AND date_key <= ?
            AND deal_name IS NOT NULL AND deal_name != ''
            ORDER BY date_key DESC, deal_name ASC
        """, (date_from, date_to))
        
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return jsonify({
                "status": "error", 
                "message": f"No cached data found for date range {date_from} to {date_to}. Please run some queries first."
            })
        
        # Organize data by provider type and aggregate by provider and date
        from collections import defaultdict
        
        # Group by provider type and provider name, then by date
        breakout_data = {
            'standard': defaultdict(lambda: defaultdict(lambda: {'imps': 0, 'oppos': 0, 'requests': 0})),
            'reseller': defaultdict(lambda: defaultdict(lambda: {'imps': 0, 'oppos': 0, 'requests': 0})),
            'vod': defaultdict(lambda: defaultdict(lambda: {'imps': 0, 'oppos': 0, 'requests': 0}))
        }
        
        for row in rows:
            date_key, deal_name, imps, oppos, total_bids, fill_rate, render_rate, requests = row
            
            # Classify provider type based on deal name using AND clauses
            deal_name_lower = deal_name.lower()
            has_reseller = 'reseller' in deal_name_lower
            has_vod = 'vod' in deal_name_lower or 'video on demand' in deal_name_lower
            
            if has_vod:
                # VOD: Contains VOD (regardless of reseller)
                provider_type = 'vod'
            elif has_reseller and not has_vod:
                # Reseller: Contains reseller AND does not contain VOD
                provider_type = 'reseller'
            else:
                # Standard/Direct: Does not contain reseller AND does not contain VOD
                provider_type = 'standard'
            
            # Aggregate data
            breakout_data[provider_type][deal_name][date_key]['imps'] += float(imps or 0)
            breakout_data[provider_type][deal_name][date_key]['oppos'] += float(oppos or 0)
            breakout_data[provider_type][deal_name][date_key]['requests'] += float(requests or 0)
        
        # Calculate fill rates and format for frontend
        formatted_data = {}
        for provider_type, providers in breakout_data.items():
            formatted_data[provider_type] = []
            
            for deal_name, dates in providers.items():
                provider_summary = {
                    'deal_name': deal_name,
                    'dates': {}
                }
                
                total_imps = 0
                total_oppos = 0
                total_requests = 0
                
                for date_key, metrics in dates.items():
                    imps = metrics['imps']
                    oppos = metrics['oppos']
                    requests = metrics['requests']
                    fill_rate = (imps / oppos * 100) if oppos > 0 else 0
                    
                    provider_summary['dates'][date_key] = {
                        'imps': imps,
                        'oppos': oppos,
                        'fill_rate': fill_rate,
                        'requests': requests
                    }
                    
                    total_imps += imps
                    total_oppos += oppos
                    total_requests += requests
                
                # Calculate overall fill rate
                overall_fill_rate = (total_imps / total_oppos * 100) if total_oppos > 0 else 0
                provider_summary['totals'] = {
                    'imps': total_imps,
                    'oppos': total_oppos,
                    'fill_rate': overall_fill_rate,
                    'requests': total_requests
                }
                
                formatted_data[provider_type].append(provider_summary)
        
        return jsonify({
            "status": "success",
            "data": formatted_data,
            "date_range": f"{date_from} to {date_to}",
            "cached": True,
            "total_rows": len(rows)
        })
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route("/debug_query", methods=["POST"])
def debug_query():
    deal_name = request.form.get("deal_name", "PSI").strip()
    
    debug_sql = f"""
    SELECT 
        f.date_key,
        d AS deal_id,
        h.name AS deal_name,
        h.id AS history_id,
        CASE WHEN h.name IS NULL THEN 'NULL_JOIN' ELSE 'FOUND_JOIN' END AS join_status,
        CAST(d AS VARCHAR) AS deal_id_string,
        CAST(h.id AS VARCHAR) AS history_id_string
    FROM
        advertising.demand_funnel f
    CROSS JOIN UNNEST(f.deal_id) AS t(d)
    LEFT JOIN ads.dim_rams_deals_history h
        ON CAST(d AS VARCHAR) = CAST(h.id AS VARCHAR)
        AND f.date_key = h.date_key
    WHERE 
        f.date_key = '2025-07-01'
        AND CONTAINS(f.demand_systems, 'PARTNER_AD_SERVER_VIDEO')
        AND (CAST(d AS VARCHAR) IN ('305zSpbZ5lpAYRjR4NBG', '4LiF9S5mkLHTSuWypPkH', '5ShHYWXvUWwup98q19Rp')
             OR h.name LIKE '%{deal_name}%')
    ORDER BY d
    LIMIT 20
    """
    
    payload = {
        "database_id": SUPERSET_DB_ID,
        "schema": "advertising", 
        "sql": debug_sql,
        "runAsync": False
    }
    
    try:
        resp = requests.post(
            SUPERSET_EXECUTE_URL,
            headers=SUPERSET_HEADERS,
            data=json.dumps(payload),
            timeout=60
        )
        
        if resp.status_code == 200:
            data = resp.json()
            return jsonify({
                "status": "success",
                "data": data.get("data", []),
                "columns": data.get("columns", []),
                "sql": debug_sql
            })
        else:
            return jsonify({"status": "error", "message": resp.text})
            
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route("/check_status/<job_id>", methods=["GET"])
def check_status(job_id):
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"status": "error", "message": "Invalid job_id"}), 404
    return jsonify(job)

if __name__ == "__main__":
    app.run(debug=True, port=5003)
