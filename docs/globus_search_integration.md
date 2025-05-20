# Globus Search Integration

SPAwn includes functionality to publish metadata to Globus Search and create web interfaces for searching and browsing the indexed content.

## Prerequisites

To use the Globus Search integration features, you need:

1. A Globus account
2. A Globus Search index
   - You can create a search index through the [Globus Search Console](https://app.globus.org/search)
   - Or use the SPAwn CLI command: `spawn search create-index --display-name "My Index"`
   - Note the UUID of your search index
3. A Globus Auth token with the appropriate scopes:
   - `search.ingest` scope for publishing metadata
   - `search.create_index` scope for creating indexes
   - You can obtain a token using the [Globus CLI](https://docs.globus.org/cli/) or the [Globus SDK](https://globus-sdk-python.readthedocs.io/)

## Configuration

You can configure Globus Search credentials and settings in the following ways:

1. In the configuration file:

```yaml
globus:
  # Globus Auth token with search.ingest scope
  auth_token: "your-globus-auth-token"
  
  # Globus Search index UUID
  search_index: "your-search-index-uuid"
  
  # List of Globus Auth identities that can see entries
  visible_to: ["public"]
```

2. Using environment variables:

```bash
export GLOBUS_AUTH_TOKEN="your-globus-auth-token"
export GLOBUS_SEARCH_INDEX="your-search-index-uuid"
```

3. Passing them directly to the CLI commands:

```bash
spawn crawl /path/to/directory --search-index "your-search-index-uuid" --auth-token "your-globus-auth-token"
```

## Publishing Metadata to Globus Search

SPAwn can crawl a directory, extract metadata from files, and publish it to Globus Search:

```bash
# Basic usage
spawn crawl /path/to/directory --search-index "your-search-index-uuid"

# Specify who can see the entries
spawn crawl /path/to/directory --search-index "your-search-index-uuid" --visible-to "public" --visible-to "urn:globus:groups:id:your-group-id"

# Save metadata to JSON files as well
spawn crawl /path/to/directory --search-index "your-search-index-uuid" --save-json --json-dir ./metadata
```

## Retrieving Entries from Globus Search

You can retrieve entries from Globus Search using the `get-entry` command:

```bash
# Get an entry by subject
spawn get-entry "file:///path/to/file" --search-index "your-search-index-uuid"

# Get information about the index
spawn get-entry --search-index "your-search-index-uuid"
```

## Globus Search Entry Format

When publishing metadata to Globus Search, SPAwn converts the extracted metadata into the GMetaEntry format:

```json
{
  "subject": "file:///path/to/file",
  "visible_to": ["public"],
  "content": {
    "filename": "example.txt",
    "extension": ".txt",
    "size_bytes": 1024,
    "created_at": "2023-01-01T12:00:00",
    "modified_at": "2023-01-02T12:00:00",
    "accessed_at": "2023-01-03T12:00:00",
    "mime_type": "text/plain",
    "encoding": "utf-8",
    "content_preview": "This is a sample text file...",
    "line_count": 10,
    "word_count": 100,
    "char_count": 500,
    "language": "en",
    "keywords": ["sample", "text", "example"]
  }
}
```

The `subject` field is a unique identifier for the entry, typically using the file path with a `file://` prefix.

The `visible_to` field controls who can see the entry. It can be set to `["public"]` to make the entry visible to everyone, or to specific Globus Auth identities or groups.

The `content` field contains the extracted metadata.

## Creating a Web Interface

After publishing metadata to Globus Search, you can create a web interface for searching and browsing the indexed content using the GitHub integration:

```bash
# Create a new portal repository
spawn github fork-portal --name "my-search-portal" --clone-dir ./portal

# Configure the portal with your Globus Search index
spawn github configure-portal ./portal --index-name "your-search-index-uuid" --title "My Search Portal" --subtitle "Search and discover data"

# Push changes to GitHub
cd ./portal
git add static.json
git commit -m "Configure portal"
git push
```

See the [GitHub Integration](github_integration.md) documentation for more details on creating and configuring the web interface.

## Example Workflow

Here's a complete workflow for creating a search index, crawling a directory, publishing metadata to Globus Search, and creating a web interface:

```bash
# Create a new Globus Search index
spawn search create-index --display-name "My Research Data Index" --description "Index for my research data"
# Note the index UUID from the output (e.g., "1234abcd-5678-efgh-9012-ijklmnopqrst")

# Crawl a directory and publish metadata to the new index
spawn crawl /path/to/data --search-index "1234abcd-5678-efgh-9012-ijklmnopqrst" --save-json

# Create a new portal repository
spawn github fork-portal --name "my-search-portal" --clone-dir ./portal

# Configure the portal with your Globus Search index
spawn github configure-portal ./portal --index-name "1234abcd-5678-efgh-9012-ijklmnopqrst" --title "My Research Data Portal" --subtitle "Search and discover research data"

# Push changes to GitHub
cd ./portal
git add static.json
git commit -m "Configure portal"
git push
```

## Advanced Configuration

For advanced configuration of the Globus Search integration, you can modify the following settings:

- `subject_prefix`: The prefix to use for the subject field in GMetaEntries
- `visible_to`: The list of Globus Auth identities that can see the entries
- `batch_size`: The number of entries to ingest in a single batch

These settings can be configured in the configuration file or passed directly to the CLI commands.

## Creating a Globus Search Index

SPAwn provides a command to create a new Globus Search index programmatically:

```bash
# Create a basic search index
spawn search create-index --display-name "My Data Index"

# Create an index with a description
spawn search create-index --display-name "My Data Index" --description "Index for my research data"

# Create an index with specific visibility
spawn search create-index --display-name "My Data Index" --visible-to "public" --visible-to "urn:globus:groups:id:your-group-id"
```

The command will output the UUID of the newly created index, which you can use in your configuration or with the `--search-index` option in other commands.

This command requires a Globus Auth token with the `search.create_index` scope. The authentication process will be handled automatically through the Globus SDK.