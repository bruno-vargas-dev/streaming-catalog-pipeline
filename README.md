# Technical Challenge - The Streaming Service’s Lost Episodes

This repository contains the solution for the Streaming Service Data Cleaning challenge. The pipeline processes a raw catalog of TV series episodes, standardizes formats, validates business rules, removes duplicates based on a quality hierarchy, and generates a clean dataset alongside a quality report.

## 📂 Repository Structure
```text
├── Technical_Challenge_Proofpoint.ipynb  # Jupyter Notebook containing the full Exploratory Data Analysis (EDA), step-by-step logic explanation, and the data profiling process.
├── data_pipeline.py                      # Production-ready data cleaning script
├── input_episodes.csv                    # Raw synthetic test dataset provided to simulate the edge cases
├── episodes_clean.csv                    # Generated output catalog (clean data)
├── report.md                             # Generated Data Quality Report
├── word_frequency.py                     # Bonus exercise C (Word Frequency Analysis)
├── sample_text.txt                       # Sample file to test the word frequency script
└── README.md                             # This documentation file

```

## ⚙️ Prerequisites

To run the script, ensure you have Python installed along with the `pandas` and `numpy` libraries.
```bash
pip install pandas numpy
```

## 🚀 How to Run the Pipeline

For the pipeline to execute successfully, the input file must be a comma-separated CSV and contain at least the following exact column headers (case-sensitive):

#### *SeriesName, SeasonNumber, EpisodeNumber, EpisodeTitle, AirDate*

> (Note: The pipeline is schema-resilient. Any additional columns present in the CSV will be safely ignored and excluded from the final output).


### Execution

The script is designed to be highly flexible. It can be executed directly from your terminal, accepting the input file path as an optional command-line argument. If you run the script without arguments, it will automatically look for `input_episodes.csv` in the same directory.

Here are the different ways you can run it:

**1. Default Execution (No arguments):**

If you run the script without any arguments, it will automatically look for `input_episodes.csv` in the same directory.
```bash
python data_pipeline.py
```

**2. Custom File in the Same Directory:**

If you have a different dataset located in the exact same folder as the script, just pass the file name.
```bash
python data_pipeline.py my_input.csv
```

**3. Using a Relative or Absolute Path:**

You can pass the relative or absolute path to a file located anywhere on your computer.
```bash
python data_pipeline.py ./data/my_input.csv
python data_pipeline.py "D:\Usuario\UTN\data\my_input.csv"
```

## 📊 Output

Upon successful execution, the script will display the execution time in the terminal and generate two files in the current working directory:

- episodes_clean.csv → The fully standardized, validated, and deduplicated catalog.

- report.md → A summary report detailing the total records processed, discarded, corrected, and duplicates removed, alongside the deduplication strategy explanation.

## 📋 What the Pipeline Does
- Parses and normalizes all fields
- Applies all business rules (defaults, discards, corrections) exactly as specified
- Removes duplicates using the three official identity rules + quality priority
- Keeps only the “best” record for each episode
- Generates a professional quality report

---

## 🌟 Bonus Exercise C — Word Frequency Analysis

A separate script (word_frequency.py) was implemented for the optional Part C of the challenge. 
It counts word frequency (case-insensitive, ignoring punctuation, accents, and special characters) and displays the top 10 most frequent words with their counts.

### Execution:

The script is designed to be highly flexible. It can be executed directly from your terminal, accepting the input file path as an optional command-line argument. If you run the script without arguments, it will automatically look for `sample_text.txt` in the same directory.

```bash
# Default execution
python word_frequency.py

# Custom text file
python word_frequency.py my_text.txt
python word_frequency.py ./data/my_text.txt
python word_frequency.py "D:\Usuario\UTN\data\my_text.txt"
```
