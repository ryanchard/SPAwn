# GitHub Integration

SPAwn includes functionality to create and configure GitHub repositories for hosting Globus Search Portals.

## Prerequisites

To use the GitHub integration features, you need:

1. A GitHub account
2. A personal access token with the `repo` scope
   - Go to GitHub Settings > Developer settings > Personal access tokens
   - Generate a new token with the `repo` scope
   - Save the token securely

## Configuration

You can configure GitHub credentials in the following ways:

1. In the configuration file:

```yaml
github:
  token: "your-github-token"
  username: "your-github-username"
```

2. Using environment variables:

```bash
export GITHUB_TOKEN="your-github-token"
export GITHUB_USERNAME="your-github-username"
```

3. Passing them directly to the CLI commands:

```bash
spawn github fork-portal --name "my-portal" --token "your-github-token" --username "your-github-username"
```

## Creating a Portal Repository

SPAwn can create a new GitHub repository by forking the [Globus Template Search Portal](https://github.com/globus/template-search-portal):

```bash
# Basic usage
spawn github fork-portal --name "my-search-portal"

# With description and organization
spawn github fork-portal --name "my-search-portal" --description "My custom search portal" --organization "my-org"

# Clone the repository locally
spawn github fork-portal --name "my-search-portal" --clone-dir ./portal
```

## Configuring the Portal

After creating the repository, you can configure the `static.json` file that controls the portal's behavior:

```bash
# Basic configuration
spawn github configure-portal ./portal --index-name my-index

# Custom index host
spawn github configure-portal ./portal --index-name my-index --index-host "https://custom-search-endpoint.org"

# Add title and subtitle
spawn github configure-portal ./portal --index-name my-index --title "My Search Portal" --subtitle "Search and discover data"

# Use a custom configuration file
spawn github configure-portal ./portal --index-name my-index --config-file ./my-config.json

# Configure and push changes to GitHub
spawn github configure-portal ./portal --index-name my-index --title "My Search Portal" --push --repo-owner "your-username" --repo-name "my-search-portal"
```

## Example Workflow

Here's a complete workflow for creating and configuring a search portal:

```bash
# Crawl a directory and publish metadata to Globus Search
spawn crawl /path/to/data --save-json --search-index "your-search-index-uuid"

# Create a new portal repository
spawn github fork-portal --name "my-search-portal" --clone-dir ./portal

# Configure the portal and push changes to GitHub
spawn github configure-portal ./portal --index-name "your-search-index-uuid" --title "My Search Portal" --subtitle "Search and discover data" --push --repo-owner "your-username" --repo-name "my-search-portal"
```

## Advanced Configuration

For advanced portal configuration, you can create a custom JSON file with additional settings:

```json
{
  "search": {
    "filters": [
      {
        "field": "keywords",
        "label": "Keywords",
        "type": "multiselect"
      },
      {
        "field": "file_type",
        "label": "File Type",
        "type": "multiselect"
      }
    ],
    "facets": [
      {
        "field": "keywords",
        "label": "Keywords"
      },
      {
        "field": "file_type",
        "label": "File Type"
      }
    ],
    "results_per_page": 10
  }
}
```

Then use this file when configuring the portal:

```bash
spawn github configure-portal ./portal --index-name my-index --config-file ./my-config.json
```

See the [examples/static.json.example](../examples/static.json.example) file for a complete example of the configuration options.