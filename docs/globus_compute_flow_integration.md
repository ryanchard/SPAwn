# Globus Compute and Flow Integration

SPAwn includes functionality to remotely crawl directories using Globus Compute and orchestrate the entire process using Globus Flows.

## Prerequisites

To use the Globus Compute and Flow integration features, you need:

1. A Globus account
2. A Globus Compute endpoint
   - You can create a Compute endpoint using the [Globus Compute CLI](https://globus-compute.readthedocs.io/en/latest/endpoints.html)
   - Note the UUID of your Compute endpoint
3. A Globus Auth token with the appropriate scopes
   - For Compute: `https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all`
   - For Flow: `https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/manage_flows`
4. (Optional) A Globus Auth client ID and secret for creating flows
   - You can create a client in the [Globus Developers Console](https://developers.globus.org/)

## Installation

To use the Globus Compute and Flow integration, you need to install the required dependencies:

```bash
# For Globus Compute integration
pip install -e ".[compute]"

# For Globus Flow integration
pip install -e ".[flow]"

# For both
pip install -e ".[compute,flow]"
```

## Configuration

You can configure Globus Compute and Flow credentials and settings in the following ways:

1. In the configuration file:

```yaml
globus:
  # Globus Auth token
  auth_token: "your-globus-auth-token"
  
  # Globus Compute endpoint ID
  compute_endpoint_id: "your-compute-endpoint-id"
  
  # Globus Flow ID (if you have an existing flow)
  flow_id: "your-flow-id"
  
  # Globus Auth client ID and secret (for creating flows)
  client_id: "your-client-id"
  client_secret: "your-client-secret"
```

2. Using environment variables:

```bash
export GLOBUS_AUTH_TOKEN="your-globus-auth-token"
export GLOBUS_COMPUTE_ENDPOINT_ID="your-compute-endpoint-id"
export GLOBUS_FLOW_ID="your-flow-id"
export GLOBUS_CLIENT_ID="your-client-id"
export GLOBUS_CLIENT_SECRET="your-client-secret"
```

3. Passing them directly to the CLI commands:

```bash
spawn compute remote-crawl /path/to/directory --endpoint-id "your-compute-endpoint-id"
```

## Remote Crawling with Globus Compute

SPAwn can crawl a directory on a remote filesystem using Globus Compute:

```bash
# Basic usage
spawn compute remote-crawl /path/to/directory --endpoint-id "your-compute-endpoint-id"

# Save metadata to JSON files
spawn compute remote-crawl /path/to/directory --endpoint-id "your-compute-endpoint-id" --save-json --json-dir ./metadata

# Publish metadata to Globus Search
spawn compute remote-crawl /path/to/directory --endpoint-id "your-compute-endpoint-id" --search-index "your-search-index-uuid"

# Run asynchronously and get the task ID
spawn compute remote-crawl /path/to/directory --endpoint-id "your-compute-endpoint-id" --no-wait

# Get the result of a task
spawn compute get-result "your-task-id"
```

### How Remote Crawling Works

1. SPAwn registers a function with Globus Compute that can crawl a directory and extract metadata
2. The function is executed on the Globus Compute endpoint
3. The function returns the extracted metadata
4. SPAwn can then save the metadata to JSON files or publish it to Globus Search

## Orchestrating with Globus Flows

SPAwn can create and run Globus Flows to orchestrate the entire process of crawling, indexing, and portal creation:

```bash
# Create a new flow
spawn flow create --client-id "your-client-id" --client-secret "your-client-secret"

# Run a flow
spawn flow run --compute-endpoint-id "your-compute-endpoint-id" --directory "/path/to/directory" --search-index "your-search-index-uuid" --portal-name "my-search-portal" --portal-title "My Search Portal"

# Run a flow and wait for it to complete
spawn flow run --compute-endpoint-id "your-compute-endpoint-id" --directory "/path/to/directory" --search-index "your-search-index-uuid" --portal-name "my-search-portal" --portal-title "My Search Portal" --wait
```

### Flow Definition

The SPAwn Flow consists of the following steps:

1. **CrawlDirectory**: Crawl a directory on a remote filesystem using Globus Compute
2. **PublishToSearch**: Publish the extracted metadata to Globus Search
3. **CreatePortal**: Fork the Globus template search portal
4. **ConfigurePortal**: Configure the portal with the Globus Search index and push the changes to GitHub

## Example Workflow

Here's a complete workflow for crawling a directory on a remote filesystem, publishing metadata to Globus Search, and creating a web interface:

```bash
# Create a flow
spawn flow create

# Run the flow
spawn flow run \
  --compute-endpoint-id "your-compute-endpoint-id" \
  --directory "/path/to/directory" \
  --search-index "your-search-index-uuid" \
  --portal-name "my-search-portal" \
  --portal-title "My Search Portal" \
  --portal-subtitle "Search and discover data" \
  --github-token "your-github-token" \
  --github-username "your-github-username"
```

## Advanced Configuration

For advanced configuration of the Globus Compute and Flow integration, you can modify the following settings:

- **Compute**:
  - `exclude_patterns`: Glob patterns to exclude from crawling
  - `include_patterns`: Glob patterns to include in crawling
  - `exclude_regex`: Regex patterns to exclude from crawling
  - `include_regex`: Regex patterns to include in crawling
  - `max_depth`: Maximum depth to crawl
  - `follow_symlinks`: Whether to follow symbolic links
  - `polling_rate`: Time in seconds to wait between file operations
  - `ignore_dot_dirs`: Whether to ignore directories starting with a dot

- **Flow**:
  - `visible_to`: Globus Auth identities that can see entries in Globus Search
  - `timeout`: Timeout in seconds for waiting for the flow to complete

These settings can be configured in the configuration file or passed directly to the CLI commands.

## Troubleshooting

### Globus Compute Issues

- **Function registration fails**: Ensure you have the correct permissions on the Globus Compute endpoint
- **Function execution fails**: Check the logs on the Globus Compute endpoint
- **Task times out**: Increase the timeout value with the `--timeout` option

### Globus Flow Issues

- **Flow creation fails**: Ensure your client ID and secret have the correct permissions
- **Flow execution fails**: Check the flow status with the Globus Automate CLI
- **Flow times out**: Increase the timeout value with the `--timeout` option