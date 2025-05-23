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

Get started by picking one of the strategies below:

* [Ingest Files Locally](docs/globus_search_integration.md)
* [Ingest Files Remotely with Globus Compute](docs/globus_compute_flow_integration.md)

Then follow this doc to build your portal: 

* [Configure and Build your SPA Portal](docs/github_integration.md)

## Configuration

SPAwn can be configured using a YAML configuration file. See `config.example.yaml` for details.

## License

See the [LICENSE](LICENSE) file for details.
