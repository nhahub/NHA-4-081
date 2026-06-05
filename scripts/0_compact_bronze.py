"""
0_compact_bronze.py — Bronze Layer Compaction

Prevents the Spark "small files problem" by merging old batch files into
monthly archive files. Runs every 3 days via the Airflow DAG.

Strategy:
  - Files < COMPACT_AGE_DAYS old are left untouched (recent batches).
  - Files >= COMPACT_AGE_DAYS old are grouped by YYYY_MM.
  - Each group is merged into a single archive file using a streaming
    writer (no RAM spike — records are written one at a time).
  - Original files are deleted after successful archiving.

Result:
  data/bronze/
  ├── store_raw_archive_2026_04.json   ← all of April merged
  ├── store_raw_archive_2026_05.json   ← all of May merged
  ├── store_raw_20260601_010000.json   ← recent (kept)
  └── store_raw_20260605_010900.json   ← latest (kept)
"""
import os
import json
import glob
import re
from datetime import datetime, timedelta
from collections import defaultdict

# ============================================================
# Configuration
# ============================================================
BRONZE_DIR       = "data/bronze"
COMPACT_AGE_DAYS = int(os.getenv("COMPACT_AGE_DAYS", "7"))  # Leave last N days untouched

# Regex to extract timestamp from filenames like store_raw_20260605_010900.json
# Does NOT match archive files (store_raw_archive_*.json) — those are already compacted
BATCH_PATTERN = re.compile(
    r"^(store_raw|reviews_raw)_(\d{4})(\d{2})(\d{2})_\d{6}\.json$"
)

def parse_batch_files(bronze_dir):
    """
    Scan bronze_dir and return a dict of:
      { prefix: { "YYYY_MM": [filepath, ...] } }
    Only includes files older than COMPACT_AGE_DAYS (safe to compact).
    """
    cutoff = datetime.utcnow() - timedelta(days=COMPACT_AGE_DAYS)
    groups = defaultdict(lambda: defaultdict(list))

    for filepath in glob.glob(os.path.join(bronze_dir, "*.json")):
        filename = os.path.basename(filepath)
        m = BATCH_PATTERN.match(filename)
        if not m:
            continue  # Skip archive files and anything unexpected

        prefix, year, month, day = m.group(1), m.group(2), m.group(3), m.group(4)
        file_date = datetime(int(year), int(month), int(day))

        if file_date >= cutoff:
            continue  # Too recent — leave it alone

        month_key = f"{year}_{month}"
        groups[prefix][month_key].append(filepath)

    return groups


def stream_merge(input_files, output_path):
    """
    Merge a list of JSON array files into one output file using a streaming
    writer — avoids loading everything into RAM simultaneously.
    Returns the total number of records written.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    total = 0

    with open(output_path, "w", encoding="utf-8") as out:
        out.write("[\n")
        first_record = True

        for filepath in sorted(input_files):
            with open(filepath, "r", encoding="utf-8") as f:
                try:
                    records = json.load(f)
                except json.JSONDecodeError:
                    print(f"   ⚠️ Skipping malformed file: {filepath}")
                    continue

                for record in records:
                    if not first_record:
                        out.write(",\n")
                    out.write(json.dumps(record, ensure_ascii=False))
                    first_record = False
                    total += 1

        out.write("\n]")

    return total


def compact_bronze():
    print("🗜️  [COMPACT] Starting Bronze compaction...")
    print(f"   📅 Compacting files older than {COMPACT_AGE_DAYS} days")

    groups = parse_batch_files(BRONZE_DIR)

    if not any(groups.values()):
        print("   ✅ Nothing to compact — all files are recent or already archived.")
        return

    total_merged   = 0
    total_deleted  = 0
    total_archives = 0

    for prefix, month_groups in groups.items():
        for month_key, files in month_groups.items():
            if not files:
                continue

            archive_name = f"{prefix}_archive_{month_key}.json"
            archive_path = os.path.join(BRONZE_DIR, archive_name)

            # If an archive already exists for this month, we need to merge
            # the existing archive + new files together, then replace it.
            existing_archive = []
            if os.path.exists(archive_path):
                existing_archive = [archive_path]
                archive_path_tmp = archive_path + ".tmp"
                records_written = stream_merge(existing_archive + files, archive_path_tmp)
                os.replace(archive_path_tmp, archive_path)
            else:
                records_written = stream_merge(files, archive_path)

            print(f"   📦 [{prefix}] {month_key}: {len(files)} files → {archive_name} ({records_written:,} records)")

            # Delete original batch files (archive is safely written first)
            for f in files:
                os.remove(f)
                total_deleted += 1

            total_merged   += len(files)
            total_archives += 1

    print(f"\n✅ [COMPACT DONE]")
    print(f"   🗑️  {total_deleted} batch files deleted")
    print(f"   📦 {total_archives} archive files created/updated")


if __name__ == "__main__":
    compact_bronze()
