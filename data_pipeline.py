import pandas as pd
import numpy as np
import re
import unicodedata
import os
import time
import sys
from datetime import datetime

# ==========================================
# 1. CONSTANTS & HELPER FUNCTIONS
# ==========================================
SEMANTIC_EMPTY = {"n/a", "na", "none", "unknown", "--", "null"}


def is_missing(value):
    if pd.isna(value): return True
    return str(value).strip().lower() in SEMANTIC_EMPTY or str(value).strip() == ""


def clean_spaces(string):
    if pd.isna(string): return string
    return re.sub(r'\s+', ' ', str(string).strip())


def normalize_string_for_comparison(string):
    if pd.isna(string): return string
    string = str(string).strip()
    string = re.sub(r'\s+', ' ', string)
    string = unicodedata.normalize('NFKD', string)
    string = ''.join(c for c in string if not unicodedata.combining(c))
    return string.lower()


def parse_int(value):
    try:
        val = int(value)
        return val if val >= 0 else np.nan
    except:
        return np.nan


def parse_date(value):
    try:
        return datetime.strptime(str(value), "%Y-%m-%d")
    except:
        return np.nan


def clean_number(x):
    if pd.isna(x): return 0
    try:
        num = int(x)
        return num if num >= 0 else 0
    except:
        return 0


def clean_episode_title(x):
    return "Untitled Episode" if pd.isna(x) else x


def clean_air_date(x):
    if pd.isna(x):
        return "Unknown"
    if hasattr(x, 'strftime'):
        return x.strftime("%Y-%m-%d")
    return "Unknown"


# ==========================================
# 2. MAIN PIPELINE FUNCTION
# ==========================================
def process_catalog(input_file):
    # Start timer
    start_time = time.time()
    print("[INIT] Starting Streaming Service Data Cleaning Pipeline...")

    # --- DATA LOADING ---
    try:
        df_raw = pd.read_csv(input_file, delimiter=',')
        required_columns = {'SeriesName', 'SeasonNumber', 'EpisodeNumber', 'EpisodeTitle', 'AirDate'}
        if not required_columns.issubset(df_raw.columns):
            print(f"[ERROR] The file '{input_file}' does not contain the required columns.")
            print(f"Required: {required_columns}")
            return
        print(f"[LOAD] Loaded {len(df_raw)} records from '{input_file}'")
    except FileNotFoundError:
        print(f"[ERROR] File '{input_file}' not found.")
        return

    # --- INITIALIZE METRICS ---
    quality_metrics = {
        "total_input": len(df_raw),
        "total_output": 0,
        "discarded_entries": 0,
        "corrected_entries": 0,
        "duplicates_detected": 0
    }

    # --- STANDARDIZATION LAYER ---
    print("[PROC] Standardizing values...")
    df_std = df_raw.copy()

    # Unified missing values to NaN
    df_std = df_std.applymap(lambda x: np.nan if is_missing(x) else x)

    # Text fields
    df_std['SeriesName'] = df_std['SeriesName'].apply(clean_spaces)
    df_std['EpisodeTitle'] = df_std['EpisodeTitle'].apply(clean_spaces)
    df_std['SeriesName_normalized'] = df_std['SeriesName'].apply(normalize_string_for_comparison)
    df_std['EpisodeTitle_normalized'] = df_std['EpisodeTitle'].apply(normalize_string_for_comparison)

    # Numeric and Date fields
    df_std['SeasonNumber'] = df_std['SeasonNumber'].apply(parse_int)
    df_std['EpisodeNumber'] = df_std['EpisodeNumber'].apply(parse_int)
    df_std['AirDate'] = df_std['AirDate'].apply(parse_date)

    # --- DATA VALIDATION & CLEANING ---
    print("[PROC] Validating and cleaning records...")
    df_clean = df_std.copy()
    filas_corregidas = pd.Series(False, index=df_clean.index)

    # 1. Remove missing SeriesName
    initial_count = len(df_clean)
    df_clean = df_clean[df_clean['SeriesName_normalized'].notna()]
    quality_metrics["discarded_entries"] += (initial_count - len(df_clean))
    filas_corregidas = filas_corregidas.loc[df_clean.index]

    # 2. Apply defaults & track corrections
    # SeasonNumber
    cambios_season = df_clean['SeasonNumber'].isna()
    df_clean['SeasonNumber'] = df_clean['SeasonNumber'].apply(clean_number)
    filas_corregidas = filas_corregidas | cambios_season

    # EpisodeNumber
    cambios_episode = df_clean['EpisodeNumber'].isna()
    df_clean['EpisodeNumber'] = df_clean['EpisodeNumber'].apply(clean_number)
    filas_corregidas = filas_corregidas | cambios_episode

    # EpisodeTitle
    cambios_title = df_clean['EpisodeTitle'].isna()
    df_clean['EpisodeTitle'] = df_clean['EpisodeTitle'].apply(clean_episode_title)
    filas_corregidas = filas_corregidas | cambios_title

    # AirDate
    cambios_airdate = df_clean['AirDate'].isna()
    df_clean['AirDate'] = df_clean['AirDate'].apply(clean_air_date)
    filas_corregidas = filas_corregidas | cambios_airdate

    # 3. Remove records with all key fields missing
    discard_condition = (
            (df_clean['EpisodeNumber'] == 0) &
            (df_clean['EpisodeTitle'] == "Untitled Episode") &
            (df_clean['AirDate'] == "Unknown")
    )
    quality_metrics["discarded_entries"] += discard_condition.sum()
    df_clean = df_clean[~discard_condition]
    filas_corregidas = filas_corregidas[~discard_condition]

    # Save total corrected entries
    quality_metrics["corrected_entries"] = filas_corregidas.sum()

    # --- DEDUPLICATION ---
    print("[PROC] Deduplicating records...")
    initial_dedup_count = len(df_clean)

    # Sorting by Quality Priority
    df_clean['has_valid_date'] = df_clean['AirDate'].ne("Unknown")
    df_clean['has_title'] = df_clean['EpisodeTitle'].ne("Untitled Episode")
    df_clean['has_valid_se_and_ep'] = df_clean['SeasonNumber'].gt(0) & df_clean['EpisodeNumber'].gt(0)
    df_clean['original_order'] = range(len(df_clean))

    df_clean = df_clean.sort_values(
        by=['has_valid_date', 'has_title', 'has_valid_se_and_ep', 'original_order'],
        ascending=[False, False, False, True]
    )
    df_clean = df_clean.drop(columns=['has_valid_date', 'has_title', 'has_valid_se_and_ep', 'original_order'])

    # Apply Rule 1
    df_clean = df_clean.drop_duplicates(subset=['SeriesName_normalized', 'SeasonNumber', 'EpisodeNumber'], keep='first')

    # Apply Rule 2 (Missing Season)
    to_drop_r2 = df_clean.duplicated(subset=['SeriesName_normalized', 'EpisodeNumber', 'EpisodeTitle_normalized'],
                                     keep='first') & (df_clean['SeasonNumber'] == 0)
    df_clean = df_clean[~to_drop_r2]

    # Apply Rule 3 (Missing Episode)
    to_drop_r3 = df_clean.duplicated(subset=['SeriesName_normalized', 'SeasonNumber', 'EpisodeTitle_normalized'],
                                     keep='first') & (df_clean['EpisodeNumber'] == 0)
    df_clean = df_clean[~to_drop_r3]

    # Save metrics
    final_dedup_count = len(df_clean)
    quality_metrics["duplicates_detected"] = initial_dedup_count - final_dedup_count
    quality_metrics["total_output"] = final_dedup_count

    # Define output file names
    output_csv = "episodes_clean.csv"
    output_report = "report.md"

    # Ensure output paths are absolute for clarity in the final message
    output_csv_path = os.path.abspath(output_csv)
    output_report_path = os.path.abspath(output_report)

    # --- EXPORT CLEAN CSV ---
    print("[SAVE] Generating episodes_clean.csv...")
    final_df = df_clean[['SeriesName', 'SeasonNumber', 'EpisodeNumber', 'EpisodeTitle', 'AirDate']]
    final_df = final_df.sort_values(by=['SeriesName', 'SeasonNumber', 'EpisodeNumber'])
    final_df.to_csv(output_csv, index=False)

    # --- GENERATE REPORT.MD ---
    print("[REPORT] Generating report.md...")
    report_content = f"""# Data Quality Report

## 📊 Summary of Metrics
- **Total number of input records:** {quality_metrics['total_input']}
- **Total number of output records:** {quality_metrics['total_output']}
- **Number of discarded entries:** {quality_metrics['discarded_entries']}
- **Number of corrected entries:** {quality_metrics['corrected_entries']}
- **Number of duplicates detected:** {quality_metrics['duplicates_detected']}

## 🛠️ Deduplication Strategy Explanation
The deduplication process was designed to ensure that the most complete and accurate version of an episode is retained while strictly preventing the accidental deletion of legitimate, distinct episodes. 

The strategy was executed in two main phases:

**Phase 1: Prioritization by Data Quality**
Before identifying duplicates, the entire dataset was sorted based on a quality hierarchy. Records were prioritized using the following criteria (in order of importance):
1. Having a valid Air Date (over "Unknown").
2. Having a known Episode Title (over "Untitled Episode").
3. Having a valid Season Number and Episode Number (>0).
4. If still tied, keeping the first entry encountered in the file.

By doing this, whenever duplicates were found, keeping the `first` occurrence guaranteed that the best available record survived.

**Phase 2: Targeted Rule Application**
Duplicates were identified using normalized text fields (lowercased, trimmed, and without diacritics) for accurate comparison. The three business rules were applied with a targeted approach:
- **Rule 1 (Exact Match):** Duplicates sharing `(SeriesName, SeasonNumber, EpisodeNumber)` were removed directly, keeping the best record.
- **Rule 2 (Missing Season):** Duplicates sharing `(SeriesName, EpisodeNumber, EpisodeTitle)` were grouped. However, to avoid merging distinct seasons that share an episode name (e.g., "Pilot"), a record was only deleted if its `SeasonNumber` was 0 (missing).
- **Rule 3 (Missing Episode):** Duplicates sharing `(SeriesName, SeasonNumber, EpisodeTitle)` were grouped. Similarly, a record was only deleted if its `EpisodeNumber` was 0 (missing).

This targeted approach successfully removed redundancies while protecting the integrity of the catalog.
"""
    with open(output_report, "w", encoding="utf-8") as file:
        file.write(report_content)

    # End timer
    end_time = time.time()

    execution_time = round(end_time - start_time, 2)
    print(f"[TIME] Total execution time: {execution_time}s")
    print(f"[OK] Process completed successfully! Output files created:\n"
          f"    - {output_csv_path}\n"
          f"    - {output_report_path}\n\n")

# ==========================================
# 3. SCRIPT EXECUTION
# ==========================================
if __name__ == "__main__":
    # If user provides an input file as an argument, use it
    # Otherwise, default to "input_episodes.csv" (test dataset file provided with the solution)
    if len(sys.argv) > 1:
        INPUT_FILE_NAME = sys.argv[1]
    else:
        print("[WARN] No input file provided.")
        print("Example: python data_pipeline.py input_episodes.csv")
        print("Defaulting to 'input_episodes.csv' for demonstration purposes.")
        INPUT_FILE_NAME = "input_episodes.csv"

    process_catalog(INPUT_FILE_NAME)