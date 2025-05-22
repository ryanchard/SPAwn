"""
Test script for the YAML metadata extractor.
"""

import yaml
import logging
import os
import sys
from pathlib import Path

# Add the project root to the Python path
# This allows the test to find the spawn package when run from the tests directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Configure logging
logging.basicConfig(level=logging.INFO)

# Create a test YAML file
test_yaml_path = Path("test_data.yaml")
test_yaml_data = {
    "name": "Test Data",
    "version": 1.0,
    "enabled": True,
    "tags": ["test", "yaml", "metadata"],
    "config": {"timeout": 30, "retry": {"count": 3, "delay": 5}},
    "data": [
        {"id": 1, "value": "one"},
        {"id": 2, "value": "two"},
        {"id": 3, "value": "three"},
    ],
}

# Write test data to file
with open(test_yaml_path, "w") as f:
    yaml.dump(test_yaml_data, f, default_flow_style=False)

print(f"Created test file: {test_yaml_path}")

# Import and use the extractor
from spawn.extractors.yaml import YAMLMetadataExtractor

# Create an instance of the extractor
extractor = YAMLMetadataExtractor()

# Extract metadata
metadata = extractor.extract(test_yaml_path)

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
test_yaml_path.unlink()
print(f"\nRemoved test file: {test_yaml_path}")