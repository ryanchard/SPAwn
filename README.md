# SPAwn

**S**tatic **P**ortal **A**utomatic **w**eb i**n**dexer - Dynamically generate Globus Static Portals

## Overview

SPAwn is a tool that automatically crawls directories, extracts metadata, and publishes it to a Globus Search index. It then creates a web interface to search and browse the indexed content through a Globus Static Portal.

## Features

- Directory crawling and metadata extraction
- Globus Search integration:
  - Create search indexes programmatically
  - Publish metadata to search indexes
  - Retrieve entries from search indexes
- Globus Compute integration for remote crawling
- Globus Flow integration for orchestrating the entire process
- Configurable metadata extraction plugins
- Save metadata as JSON files
- GitHub integration for creating and configuring Globus Search Portals

## Installation

```bash
pip install -e .
```

## Usage

```bash
# Configure your crawler
spawn config --output-dir ./output

# Create a new Globus Search index
spawn search create-index --display-name "My Data Index" --description "Index for my research data"

# Run the crawler and publish to Globus Search
spawn crawl /path/to/directory --search-index "your-search-index-uuid" --save-json

# Extract metadata from a single file and save as JSON
spawn extract /path/to/file --save-json

# Get an entry from Globus Search
spawn get-entry "file:///path/to/file" --search-index "your-search-index-uuid"

# Fork the Globus template search portal
spawn github fork-portal --name "my-search-portal" --clone-dir ./portal

# Configure the portal with your index
spawn github configure-portal ./portal --index-name "your-search-index-uuid" --title "My Search Portal"

# Configure the portal and push changes to GitHub
spawn github configure-portal ./portal --index-name "your-search-index-uuid" --title "My Search Portal" --push --repo-owner "your-username" --repo-name "my-search-portal"

# Crawl a directory on a remote filesystem using Globus Compute
spawn compute remote-crawl /path/to/directory --endpoint-id "your-compute-endpoint-id" --search-index "your-search-index-uuid"

# Create a Globus Flow for SPAwn
spawn flow create

# Run a Globus Flow to orchestrate the entire process
spawn flow run --compute-endpoint-id "your-compute-endpoint-id" --directory "/path/to/directory" --search-index "your-search-index-uuid" --portal-name "my-search-portal" --portal-title "My Search Portal"
```

## Configuration

SPAwn can be configured using a YAML configuration file. See `config.example.yaml` for details.

## License

See the [LICENSE](LICENSE) file for details.
