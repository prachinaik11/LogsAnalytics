import os
import re
from prefixspan import PrefixSpan

# Function to read summary.txt from each thread_id folder
def load_summaries(base_dir):
    summaries = {}
    for folder in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, folder)
        summary_file = os.path.join(folder_path, "summary.txt")
        
        if os.path.isdir(folder_path) and os.path.exists(summary_file):
            with open(summary_file, "r") as f:
                summaries[folder] = f.read().strip().split(" ")  # Tokenize words
    
    return summaries

# Function to apply PrefixSpan on summaries
def apply_prefixspan(summaries):
    patterns = {}
    for thread_id, sequence in summaries.items():
        ps = PrefixSpan([sequence])  # Apply PrefixSpan on the sequence
        frequent_patterns = ps.frequent(1)  # Extract patterns with min support of 2
        patterns[thread_id] = [pattern[1] for pattern in frequent_patterns]
    
    return patterns

# Function to save extracted patterns in patterns.txt
def save_patterns(base_dir, patterns):
    for thread_id, pattern_list in patterns.items():
        folder_path = os.path.join(base_dir, thread_id)
        patterns_file = os.path.join(folder_path, "patterns.txt")
        
        with open(patterns_file, "w") as f:
            f.write("\n".join(pattern_list))

# Main execution function
def main(base_dir):
    summaries = load_summaries(base_dir)
    extracted_patterns = apply_prefixspan(summaries)
    save_patterns(base_dir, extracted_patterns)
    
    # Print extracted patterns for each thread_id
    print("\nExtracted Co-occurring Patterns:")
    for tid, patterns in extracted_patterns.items():
        print(f"Thread ID: {tid}\nPatterns:")
        for pattern in patterns:
            print(pattern)

# Run the script with the base directory containing thread_id folders
base_dir = "/home/trupti/Downloads/Log Analysis PE/Logiscope Material-20250114T095200Z-001/Logiscope Material/week5"  # Change this to the directory where thread_id folders are stored
main(base_dir)