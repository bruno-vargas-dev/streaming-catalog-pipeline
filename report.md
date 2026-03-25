# Data Quality Report

## 📊 Summary of Metrics
- **Total number of input records:** 143
- **Total number of output records:** 116
- **Number of discarded entries:** 6
- **Number of corrected entries:** 24
- **Number of duplicates detected:** 21

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
