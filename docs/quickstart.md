# SPAwn Quickstart Guide

This guide will help you quickly set up a Globus Search Portal for your data using SPAwn. We'll walk through the entire process from creating a search index to deploying a fully functional portal.

## Prerequisites

Before you begin, make sure you have:

1. Installed SPAwn (`pip install -e .`)
2. A Globus account (sign up at [globus.org](https://www.globus.org/) if you don't have one)
3. A directory containing files you want to index

## Step 1: Create a Globus Search Index

First, you need to create a Globus Search index where your metadata will be stored:

```bash
spawn search create-index --display-name "My Research Data"
```

This command will:
1. Authenticate with Globus (you'll be prompted to log in via your browser)
2. Create a new search index with the specified name
3. Output the UUID of the newly created index

Example output:
```
Search Index ID: 1234abcd-5678-efgh-9012-ijklmnopqrst
Display Name: My Research Data
Description: (No description)
Visible To: public

You can use this index ID in your configuration or with the --search-index option.
```

Make note of the Search Index ID as you'll need it in the next steps.

## Step 2: Crawl and Index Your Data

Next, crawl a directory to extract metadata from your files and publish it to the search index:

```bash
spawn crawl /path/to/your/data --search-index 1234abcd-5678-efgh-9012-ijklmnopqrst --save-json
```

This command will:
1. Recursively scan the specified directory
2. Extract metadata from each file using appropriate extractors
3. Publish the metadata to your Globus Search index
4. Save the metadata as JSON files locally (optional, but useful for debugging)

You can customize the crawling process with additional options:

```bash
# Exclude certain file patterns
spawn crawl /path/to/your/data --search-index 1234abcd-5678-efgh-9012-ijklmnopqrst --exclude "*.tmp" --exclude "*.log"

# Limit crawl depth
spawn crawl /path/to/your/data --search-index 1234abcd-5678-efgh-9012-ijklmnopqrst --max-depth 3

# Control who can see the entries
spawn crawl /path/to/your/data --search-index 1234abcd-5678-efgh-9012-ijklmnopqrst --visible-to "public"
```

## Step 3: Create a Portal

Finally, create a web portal to search and browse your indexed content:

```bash
spawn portal create --name "my-research-portal" --search-index 1234abcd-5678-efgh-9012-ijklmnopqrst --title "My Research Portal" --subtitle "Search and discover research data" --enable-pages
```

This command will:
1. Fork the Globus template search portal on GitHub
2. Configure it with your search index
3. Enable GitHub Pages for hosting
4. Clone the repository locally

Example output:
```
Repository URL: https://github.com/yourusername/my-research-portal
Portal URL: https://yourusername.github.io/my-research-portal
Clone path: /path/to/cloned/repository
```

The portal will be automatically configured to use your search index. You can visit the Portal URL to see your search portal in action.

## Complete Example Workflow

Here's a complete workflow from start to finish:

```bash
# Step 1: Create a search index
spawn search create-index --display-name "Climate Data Index"
# Output: Search Index ID: 1234abcd-5678-efgh-9012-ijklmnopqrst

# Step 2: Crawl and index your data
spawn crawl /path/to/climate/data --search-index 1234abcd-5678-efgh-9012-ijklmnopqrst --save-json

# Step 3: Create a portal
spawn portal create --name "climate-data-portal" --search-index 1234abcd-5678-efgh-9012-ijklmnopqrst --title "Climate Data Portal" --subtitle "Search and discover climate research data" --enable-pages
```

After completing these steps, you'll have:
1. A Globus Search index containing metadata about your files
2. A GitHub repository with your configured search portal
3. A publicly accessible web interface for searching and browsing your data

## Next Steps

- Customize your portal's appearance and behavior by editing the files in your cloned repository
- Set up automatic indexing using Globus Compute and Flows (see [Globus Compute Flow Integration](globus_compute_flow_integration.md))
- Add more advanced search features like facets and filters (see [GitHub Integration](github_integration.md))

## Troubleshooting

- If you encounter authentication issues, make sure you're logged into Globus with the correct account
- If files aren't being indexed properly, check that the appropriate extractors are available for your file types
- If your portal isn't displaying correctly, ensure GitHub Pages is properly enabled and the repository is public