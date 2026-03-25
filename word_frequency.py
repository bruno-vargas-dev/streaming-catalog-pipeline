import re
import time
import sys
from collections import Counter

# ==========================================
# 1. CONSTANTS & HELPER FUNCTIONS
# ==========================================
def clean_text(text):
    # Convert text to lowercase
    text = text.lower()
    # Extract words ignoring punctuation and special characters
    # Keep only alphabetic characters (including Spanish accents)
    words = re.findall(r'\b[a-zñáéíóú]+\b', text)
    return words


def get_top_words(words, n=10):
    # Return the top N most frequent words using Counter
    if not words:
        print("[ERROR] No valid words found in the file.")
        return []
    word_counts = Counter(words)
    return word_counts.most_common(n)


# ==========================================
# 2. MAIN FUNCTION
# ==========================================
def analyze_word_frequency(input_text):
    # Start timer
    start_time = time.time()
    print("[INIT] Starting Word Frequency Analysis...")

    # --- DATA LOADING ---
    try:
        # Read file safely with support for special characters (utf-8 encoding)
        with open(input_text, 'r', encoding='utf-8') as file:
            text = file.read()

        print(f"[LOAD] Loaded text file: '{input_text}'")
    except FileNotFoundError:
        print(f"[ERROR] File '{input_text}' not found.")
        return

    # Process text
    words = clean_text(text)
    top_10 = get_top_words(words)

    # --- SAFEGUARD: EMPTY FILES ---
    if not top_10:
        print("[ERROR] No valid words found in the file.")
        return

    # Display the results in a formatted table
    print("[PROC] Analysis completed successfully.")
    print("\n [RESULT] TOP 10 MOST FREQUENT WORDS")
    print("-" * 40)
    print(f"{'RANK':<4} {'WORD':<20} {'FREQUENCY':<10}")
    print("-" * 40)

    for rank, (word, count) in enumerate(top_10, 1):
        print(f"{rank:<4} {word:<20} {count:<10}")

    # End timer
    end_time = time.time()

    execution_time = round(end_time - start_time, 2)
    print(f"[TIME] Total execution time: {execution_time}s")
    print(f"[OK] Process completed successfully!")


# ==========================================
# 3. SCRIPT EXECUTION
# ==========================================
if __name__ == "__main__":
    # Ensure the user provides a text file as an argument
    if len(sys.argv) > 1:
        INPUT_FILE_NAME = sys.argv[1]
    else:
        # Default fallback
        print("[WARN] No input file provided.")
        print("Example: python word_frequency.py sample_text.txt")
        print("Defaulting to 'sample_text.txt' for demonstration purposes.")
        INPUT_FILE_NAME = 'sample_text.txt'

    analyze_word_frequency(INPUT_FILE_NAME)
