"""
Fitbit Takeout Local Processor — Symphonia Vitalis
====================================================
Processes all Fitbit takeout CSV folders into clean merged CSVs
ready for upload to GitHub and display in Grafana.

Usage (Windows Command Prompt):
    python process_fitbit_takeout.py "C:\\Users\\oriox\\Downloads\\Fitbit_takeout"

Replace the path with wherever your Fitbit takeout folder lives.
The script will create a subfolder called fitbit_processed/ next to it.

Output files:
    fitbit_rhr.csv          - Resting heart rate (daily)
    fitbit_hrv.csv          - Heart rate variability (daily)
    fitbit_temperature.csv  - Skin temperature deviation (nightly)
    fitbit_sleep_score.csv  - Sleep score + components (nightly)
    fitbit_stress_score.csv - Stress score (daily)
    fitbit_readiness.csv    - Daily readiness score
    fitbit_spo2.csv         - SpO2 (nightly)
    fitbit_azm.csv          - Active Zone Minutes (daily)
"""

import os, sys, csv, re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

try:
    from zoneinfo import ZoneInfo
    MELBOURNE = ZoneInfo("Australia/Melbourne")
except ImportError:
    import subprocess
    subprocess.run([sys.executable, "-m", "pip", "install", "tzdata", "--quiet"])
    from zoneinfo import ZoneInfo
    MELBOURNE = ZoneInfo("Australia/Melbourne")


def ms(dt):
    return int(dt.timestamp() * 1000)


def parse_ts(s):
    """Try many Fitbit timestamp formats, return UTC datetime or None."""
    if not s or not s.strip():
        return None
    s = s.strip()
    formats = [
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %I:%M%p",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=MELBOURNE).astimezone(timezone.utc)
        except:
            pass
    return None


def clean_float(s):
    if not s or str(s).strip() in ("", "null", "NULL", "N/A", "nan"):
        return None
    try:
        return float(str(s).replace(",", "").strip())
    except:
        return None


def load_csv_folder(folder_path, encoding="utf-8"):
    """Load and merge all CSVs in a folder into a list of dicts."""
    rows = []
    folder = Path(folder_path)
    if not folder.is_dir():
        print(f"  Folder not found: {folder_path}")
        return rows
    files = sorted(folder.glob("*.csv"))
    if not files:
        print(f"  No CSV files found in {folder.name}/")
        return rows
    print(f"  Found {len(files)} CSV files in {folder.name}/")
    for fp in files:
        try:
            with open(fp, encoding=encoding, errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(dict(row))
        except Exception as e:
            print(f"  Warning: {fp.name}: {e}")
    return rows


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  -> {Path(path).name}: {len(rows)} rows")


def find_folder(root, *candidates):
    """Find a subfolder by trying multiple name candidates."""
    for name in candidates:
        p = root / name
        if p.is_dir():
            return p
    # fuzzy: case-insensitive partial match
    for name in candidates:
        for child in root.iterdir():
            if child.is_dir() and name.lower() in child.name.lower():
                return child
    return None


def process(takeout_root_str):
    root = Path(takeout_root_str)
    if not root.is_dir():
        print(f"ERROR: folder not found: {root}")
        return

    out = root.parent / "fitbit_processed"
    out.mkdir(exist_ok=True)
    print(f"\nTakeout root : {root}")
    print(f"Output folder: {out}\n")

    # Print what folders are available for debugging
    print("Folders found in takeout root:")
    for d in sorted(root.iterdir()):
        if d.is_dir():
            csv_count = len(list(d.glob("*.csv")))
            print(f"  {d.name}/ ({csv_count} CSVs)")
    print()

    # ── BIOMETRICS / RHR ─────────────────────────────────────────────
    print("Processing Resting Heart Rate (Biometrics)...")
    folder = find_folder(root, "Biometrics", "Physical Activity_GoogleData")
    rows_out = []
    seen = set()
    if folder:
        for rec in load_csv_folder(folder):
            # Fitbit biometrics format: dateTime, value (JSON-like or plain)
            ts_str = rec.get("dateTime") or rec.get("date") or rec.get("Date") or ""
            ts = parse_ts(ts_str)
            if not ts:
                continue
            d = ts.date().isoformat()
            if d in seen:
                continue
            # Try to extract restingHeartRate from value field
            val_str = rec.get("value", "")
            rhr = None
            if val_str:
                # Sometimes it's JSON-like: {"restingHeartRate":62}
                m = re.search(r'"?restingHeartRate"?\s*:\s*([\d.]+)', str(val_str))
                if m:
                    rhr = clean_float(m.group(1))
                else:
                    rhr = clean_float(val_str)
            # Also check direct column
            if rhr is None:
                rhr = clean_float(rec.get("Resting Heart Rate") or
                                   rec.get("restingHeartRate") or
                                   rec.get("resting_heart_rate"))
            if rhr and 30 <= rhr <= 110:
                seen.add(d)
                rows_out.append({"time": ms(ts), "bpm": rhr})
    rows_out.sort(key=lambda r: r["time"])
    write_csv(out / "fitbit_rhr.csv", ["time", "bpm"], rows_out)

    # ── HEART RATE VARIABILITY ────────────────────────────────────────
    print("\nProcessing Heart Rate Variability...")
    folder = find_folder(root, "Heart Rate Variability")
    rows_out = []
    seen = set()
    if folder:
        for rec in load_csv_folder(folder):
            ts_str = rec.get("timestamp") or rec.get("dateTime") or rec.get("date") or ""
            ts = parse_ts(ts_str)
            if not ts:
                continue
            d = ts.date().isoformat()
            if d in seen:
                continue
            # rmssd is the main HRV metric
            rmssd = clean_float(rec.get("rmssd") or rec.get("RMSSD") or
                                  rec.get("value") or rec.get("dailyRmssd") or
                                  rec.get("deep_rmssd"))
            if rmssd and rmssd > 0:
                seen.add(d)
                cov = clean_float(rec.get("coverage") or rec.get("Coverage")) or ""
                rows_out.append({"time": ms(ts), "ms": rmssd, "coverage": cov})
    rows_out.sort(key=lambda r: r["time"])
    write_csv(out / "fitbit_hrv.csv", ["time", "ms", "coverage"], rows_out)

    # ── TEMPERATURE ───────────────────────────────────────────────────
    print("\nProcessing Skin Temperature...")
    folder = find_folder(root, "Temperature")
    rows_out = []
    seen = set()
    if folder:
        for rec in load_csv_folder(folder):
            ts_str = rec.get("dateTime") or rec.get("date") or rec.get("Date") or ""
            ts = parse_ts(ts_str)
            if not ts:
                continue
            d = ts.date().isoformat()
            if d in seen:
                continue
            # Two temp fields: nightly relative (deviation from baseline) and absolute
            deviation = clean_float(rec.get("nightly_temperature") or
                                     rec.get("value") or
                                     rec.get("temperature_deviation") or
                                     rec.get("relativeTemperatureDeviation"))
            absolute = clean_float(rec.get("skin_temperature_celsius") or
                                    rec.get("temperatureCelsius") or
                                    rec.get("absolute_temperature"))
            if deviation is not None or absolute is not None:
                seen.add(d)
                rows_out.append({
                    "time": ms(ts),
                    "deviation_c": deviation if deviation is not None else "",
                    "absolute_c": absolute if absolute is not None else ""
                })
    rows_out.sort(key=lambda r: r["time"])
    write_csv(out / "fitbit_temperature.csv",
              ["time", "deviation_c", "absolute_c"], rows_out)

    # ── SLEEP SCORE ───────────────────────────────────────────────────
    print("\nProcessing Sleep Score...")
    folder = find_folder(root, "Sleep Score", "Sleep")
    rows_out = []
    seen = set()
    if folder:
        for rec in load_csv_folder(folder):
            ts_str = (rec.get("timestamp") or rec.get("dateTime") or
                      rec.get("date") or rec.get("Date") or "")
            ts = parse_ts(ts_str)
            if not ts:
                continue
            d = ts.date().isoformat()
            if d in seen:
                continue
            score = clean_float(rec.get("overall_score") or rec.get("score") or
                                  rec.get("sleepScore") or rec.get("Sleep Score"))
            if score and score > 0:
                seen.add(d)
                rows_out.append({
                    "time": ms(ts),
                    "score": score,
                    "composition": clean_float(rec.get("composition_score")) or "",
                    "revitalization": clean_float(rec.get("revitalization_score")) or "",
                    "duration": clean_float(rec.get("duration_score")) or "",
                    "deep_min": clean_float(rec.get("deep_sleep_in_minutes")) or "",
                    "rhr": clean_float(rec.get("resting_heart_rate")) or "",
                    "restlessness": clean_float(rec.get("restlessness")) or "",
                })
    rows_out.sort(key=lambda r: r["time"])
    write_csv(out / "fitbit_sleep_score.csv",
              ["time", "score", "composition", "revitalization",
               "duration", "deep_min", "rhr", "restlessness"], rows_out)

    # ── STRESS SCORE ──────────────────────────────────────────────────
    print("\nProcessing Stress Score...")
    folder = find_folder(root, "Stress Score")
    rows_out = []
    seen = set()
    if folder:
        for rec in load_csv_folder(folder):
            ts_str = (rec.get("DATE") or rec.get("date") or
                      rec.get("dateTime") or rec.get("timestamp") or "")
            ts = parse_ts(ts_str)
            if not ts:
                continue
            d = ts.date().isoformat()
            if d in seen:
                continue
            score = clean_float(rec.get("STRESS_SCORE") or rec.get("stress_score") or
                                  rec.get("value") or rec.get("Score"))
            if score is not None:
                seen.add(d)
                qualifier = (rec.get("STRESS_QUALIFIER") or
                             rec.get("stress_qualifier") or "").strip()
                rows_out.append({"time": ms(ts), "score": score,
                                  "qualifier": qualifier})
    rows_out.sort(key=lambda r: r["time"])
    write_csv(out / "fitbit_stress_score.csv",
              ["time", "score", "qualifier"], rows_out)

    # ── DAILY READINESS ───────────────────────────────────────────────
    print("\nProcessing Daily Readiness...")
    folder = find_folder(root, "Daily Readiness")
    rows_out = []
    seen = set()
    if folder:
        for rec in load_csv_folder(folder):
            ts_str = (rec.get("date") or rec.get("dateTime") or
                      rec.get("timestamp") or rec.get("Date") or "")
            ts = parse_ts(ts_str)
            if not ts:
                continue
            d = ts.date().isoformat()
            if d in seen:
                continue
            score = clean_float(rec.get("score") or rec.get("Score") or
                                  rec.get("readiness_score") or rec.get("value"))
            if score is not None:
                seen.add(d)
                rows_out.append({"time": ms(ts), "score": score})
    rows_out.sort(key=lambda r: r["time"])
    write_csv(out / "fitbit_readiness.csv", ["time", "score"], rows_out)

    # ── SPO2 ──────────────────────────────────────────────────────────
    print("\nProcessing SpO2...")
    folder = find_folder(root, "Oxygen Saturation (SpO2)", "Oxygen Saturation")
    rows_out = []
    seen = set()
    if folder:
        for rec in load_csv_folder(folder):
            ts_str = (rec.get("timestamp") or rec.get("dateTime") or
                      rec.get("date") or rec.get("Date") or "")
            ts = parse_ts(ts_str)
            if not ts:
                continue
            # Fitbit SpO2: avg, min, max per night
            avg = clean_float(rec.get("avg_spo2") or rec.get("average_spo2") or
                               rec.get("value") or rec.get("SpO2"))
            mn = clean_float(rec.get("min_spo2") or rec.get("minimum_spo2"))
            mx = clean_float(rec.get("max_spo2") or rec.get("maximum_spo2"))
            key = ts.strftime("%Y-%m-%d-%H-%M")
            if key in seen:
                continue
            if avg and 80 <= avg <= 100:
                seen.add(key)
                rows_out.append({
                    "time": ms(ts),
                    "avg_pct": avg,
                    "min_pct": mn if mn else "",
                    "max_pct": mx if mx else "",
                })
    rows_out.sort(key=lambda r: r["time"])
    write_csv(out / "fitbit_spo2.csv",
              ["time", "avg_pct", "min_pct", "max_pct"], rows_out)

    # ── ACTIVE ZONE MINUTES ───────────────────────────────────────────
    print("\nProcessing Active Zone Minutes...")
    folder = find_folder(root, "Active Zone Minutes (AZM)", "Active Zone Minutes")
    rows_out = []
    seen = set()
    if folder:
        for rec in load_csv_folder(folder):
            ts_str = (rec.get("date_time") or rec.get("dateTime") or
                      rec.get("date") or rec.get("Date") or "")
            ts = parse_ts(ts_str)
            if not ts:
                continue
            d = ts.date().isoformat()
            if d in seen:
                continue
            total = clean_float(rec.get("total_minutes") or rec.get("activeZoneMinutes") or
                                  rec.get("value"))
            if total is not None:
                seen.add(d)
                rows_out.append({
                    "time": ms(ts),
                    "total_min": total,
                    "fat_burn_min": clean_float(rec.get("fat_burn_active_zone_minutes")) or "",
                    "cardio_min": clean_float(rec.get("cardio_active_zone_minutes")) or "",
                    "peak_min": clean_float(rec.get("peak_active_zone_minutes")) or "",
                })
    rows_out.sort(key=lambda r: r["time"])
    write_csv(out / "fitbit_azm.csv",
              ["time", "total_min", "fat_burn_min", "cardio_min", "peak_min"], rows_out)

    # ── SUMMARY ───────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(f"Done! Output files in: {out}")
    print(f"\nNext steps:")
    print(f"1. Check the output files look reasonable (open in Excel/Notepad)")
    print(f"2. Upload them to your GitHub repo symphonia-vitalis-data-v1")
    print(f"3. Tell Claude which files were generated so panels can be added")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nERROR: Please provide the path to your Fitbit takeout folder.")
        print('Example: python process_fitbit_takeout.py "C:\\Users\\oriox\\Downloads\\Fitbit_takeout"')
        sys.exit(1)
    process(sys.argv[1])
