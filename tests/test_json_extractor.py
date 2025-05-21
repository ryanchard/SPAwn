"""
Test script for the JSON metadata extractor.
"""

import json
import logging
import os
import sys
from pathlib import Path

# Add the project root to the Python path
# This allows the test to find the spawn package when run from the tests directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create a test JSON file
test_json_path = Path("test_data.json")
test_json_data = {
    "name": "Test Data",
    "version": 1.0,
    "enabled": True,
    "tags": ["test", "json", "metadata"],
    "config": {
        "timeout": 30,
        "retry": {
            "count": 3,
            "delay": 5
        }
    },
    "data": [
        {"id": 1, "value": "one"},
        {"id": 2, "value": "two"},
        {"id": 3, "value": "three"}
    ]
}

# Write test data to file
with open(test_json_path, "w") as f:
    json.dump(test_json_data, f, indent=2)

print(f"Created test file: {test_json_path}")

# Import and use the extractor
from spawn.extractors.json import JSONMetadataExtractor

# Create an instance of the extractor
extractor = JSONMetadataExtractor()

# Extract metadata
metadata = extractor.extract(test_json_path)

# Print the metadata
print("\nExtracted Metadata:")
for key, value in metadata.items():
    print(f"{key}: {value}")

# Explicitly check for the file metadata
if "file" in metadata:
    print("\nFile Metadata:")
    for key, value in metadata["file"].items():
        print(f"  {key}: {value}")
else:
    print("\nWarning: 'file' key not found in metadata!")

# Clean up
test_json_path.unlink()
print(f"\nRemoved test file: {test_json_path}")