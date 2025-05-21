"""
Command-line interface for SPAwn.

This module provides a command-line interface for the SPAwn tool.
"""

import logging
import json
import sys
from pathlib import Path
from typing import List, Optional

import globus_sdk
from globus_sdk import UserApp, ClientApp
from globus_sdk import SearchClient, FlowsClient

import click

from spawn.config import config, load_config
from spawn.crawler import crawl_directory
from spawn.github import create_template_portal, configure_static_json
from spawn.globus_compute import (
    remote_crawl,
    remote_ingest_metadata,
    register_functions,
    get_task_result,
    create_portal_remotely,
)
from spawn.globus_flow import create_and_run_flow, SPAwnFlow
from spawn.globus_search import publish_metadata, GlobusSearchClient
from spawn.metadata import extract_metadata

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "--config-file",
    "-c",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to configuration file",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.version_option()
def cli(config_file: Optional[Path], verbose: bool):
    """
    SPAwn - Static Portal Automatic web indexer.

    A tool for crawling directories, extracting metadata, and generating
    Globus Static Portals from Elasticsearch indices.
    """
    # Configure logging level
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose output enabled")

    # Load configuration
    if config_file:
        try:
            load_config(config_file)
            logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            sys.exit(1)


@cli.command()
@click.argument(
    "directory", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option(
    "--exclude",
    "-e",
    multiple=True,
    help="Glob pattern to exclude from crawling (can be used multiple times)",
)
@click.option(
    "--include",
    "-i",
    multiple=True,
    help="Glob pattern to include in crawling (can be used multiple times)",
)
@click.option(
    "--exclude-regex",
    "-E",
    multiple=True,
    help="Regex pattern to exclude from crawling (can be used multiple times)",
)
@click.option(
    "--include-regex",
    "-I",
    multiple=True,
    help="Regex pattern to include in crawling (can be used multiple times)",
)
@click.option(
    "--max-depth",
    "-d",
    type=int,
    help="Maximum depth to crawl",
)
@click.option(
    "--follow-symlinks/--no-follow-symlinks",
    default=False,
    help="Whether to follow symbolic links",
)
@click.option(
    "--polling-rate",
    "-p",
    type=float,
    help="Time in seconds to wait between file operations",
)
@click.option(
    "--ignore-dot-dirs/--include-dot-dirs",
    default=True,
    help="Whether to ignore directories starting with a dot",
)
@click.option(
    "--search-index",
    help="Globus Search index UUID",
)
@click.option(
    "--visible-to",
    multiple=True,
    help="Globus Auth identities that can see entries (can be used multiple times)",
)
@click.option(
    "--save-json/--no-save-json",
    default=None,
    help="Whether to save metadata as JSON files",
)
@click.option(
    "--json-dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory to save JSON metadata files in",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Only list files that would be indexed, without actually indexing them",
)
def crawl(
    directory: Path,
    exclude: List[str],
    include: List[str],
    exclude_regex: List[str],
    include_regex: List[str],
    max_depth: Optional[int],
    follow_symlinks: bool,
    polling_rate: Optional[float],
    ignore_dot_dirs: bool,
    search_index: Optional[str],
    visible_to: List[str],
    save_json: Optional[bool],
    json_dir: Optional[Path],
    dry_run: bool,
):
    """
    Crawl a directory and index discovered files.

    DIRECTORY is the path to the directory to crawl.
    """
    logger.info(f"Crawling directory: {directory}")

    # Use command-line options or fall back to config values
    exclude_patterns = list(exclude) if exclude else None
    include_patterns = list(include) if include else None
    exclude_regex_patterns = list(exclude_regex) if exclude_regex else None
    include_regex_patterns = list(include_regex) if include_regex else None

    # Crawl directory
    files = crawl_directory(
        directory,
        exclude_patterns=exclude_patterns,
        include_patterns=include_patterns,
        exclude_regex=exclude_regex_patterns,
        include_regex=include_regex_patterns,
        max_depth=max_depth,
        follow_symlinks=follow_symlinks,
        polling_rate=polling_rate,
        ignore_dot_dirs=ignore_dot_dirs,
    )

    logger.info(f"Discovered {len(files)} files")

    if dry_run:
        # Just print the files that would be indexed
        for file in files:
            print(file)
        return

    metadata = {}

    # Save metadata to JSON if requested
    if save_json:
        from spawn.metadata import extract_metadata, save_metadata_to_json

        logger.info("Saving metadata to JSON files")
        json_count = 0

        for file_path in files:
            try:
                metadata[str(file_path.absolute())] = extract_metadata(file_path)
                json_count += 1
            except Exception as e:
                logger.error(f"Error saving metadata for {file_path}: {e}")

        json_path = save_metadata_to_json(metadata, output_dir=json_dir)
        logger.info(f"Saved metadata for {json_count} files to JSON at {json_path}")

    # Get search index from options or config
    index_uuid = search_index or config.globus_search_index
    if not index_uuid:
        logger.error("No Globus Search index UUID provided")
        sys.exit(1)

    client = GlobusSearchClient(
        index_uuid=search_index,
    )

    # Get visible_to from options or config
    visible_to_list = (
        list(visible_to) if visible_to else config.globus_search_visible_to
    )

    # Publish metadata to Globus Search
    logger.info(f"Publishing metadata to Globus Search index: {index_uuid}")

    result = publish_metadata(
        metadata=metadata,
        index_uuid=index_uuid,
        visible_to=visible_to_list,
    )

    logger.info(
        f"Published {result['success']} entries, failed to publish {result['failed']} entries"
    )


@cli.command()
@click.argument("subject", required=False)
@click.option(
    "--search-index",
    help="Globus Search index UUID",
)
def get_entry(
    subject: Optional[str],
    search_index: Optional[str],
):
    """
    Get an entry from Globus Search.

    If SUBJECT is provided, gets the entry with that subject.
    Otherwise, prints information about the index.
    """
    # Get search index from options or config
    index_uuid = search_index or config.globus_search_index
    if not index_uuid:
        logger.error("No Globus Search index UUID provided")
        sys.exit(1)

    client = GlobusSearchClient(index_uuid=index_uuid)

    if subject:
        # Get entry by subject
        entry = client.get_entry(index_uuid, subject)

        if entry:
            import json

            print(json.dumps(entry, indent=2, default=str))
        else:
            print(f"No entry found with subject: {subject}")
    else:
        # Print information about the index
        print(f"Globus Search Index: {index_uuid}")
        print("Use --subject to get a specific entry")


@cli.command(name="extract")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "--save-json/--no-save-json",
    default=False,
    help="Whether to save metadata as JSON file",
)
@click.option(
    "--json-dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory to save JSON metadata file in",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Path to save JSON metadata file (overrides --json-dir)",
)
def extract_file_metadata(
    file: Path, save_json: bool, json_dir: Optional[Path], output: Optional[Path]
):
    """
    Extract metadata from a single file.

    FILE is the path to the file to extract metadata from.
    """
    metadata = extract_metadata(file)

    # Print metadata as JSON
    import json

    print(json.dumps(metadata, indent=2, default=str))

    # Save metadata to JSON file if requested
    if save_json or output:
        if output:
            # Use specified output path
            output_path = output
            output_dir = output.parent
            output_dir.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(metadata, f, indent=2, default=str)
            print(f"\nMetadata saved to: {output_path}")
        else:
            # Use save_metadata_to_json function
            try:
                json_path = save_metadata_to_json(file, metadata, output_dir=json_dir)
                print(f"\nMetadata saved to: {json_path}")
            except Exception as e:
                print(f"\nError saving metadata to JSON: {e}")


@cli.group()
def github():
    """
    GitHub repository operations.
    """
    pass


@github.command(name="fork-portal")
@click.option(
    "--name",
    required=True,
    help="Name for the forked repository",
)
@click.option(
    "--description",
    help="Description for the new repository",
)
@click.option(
    "--organization",
    help="Organization to create the fork in",
)
@click.option(
    "--token",
    help="GitHub personal access token (overrides config and environment)",
)
@click.option(
    "--username",
    help="GitHub username (overrides config and environment)",
)
@click.option(
    "--clone-dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory to clone the repository into",
)
def fork_portal(
    name: str,
    description: Optional[str],
    organization: Optional[str],
    token: Optional[str],
    username: Optional[str],
    clone_dir: Optional[Path],
):
    """
    Fork the Globus template search portal.

    Creates a new GitHub repository by forking the Globus template search portal.
    Requires a GitHub personal access token with 'repo' scope.
    """
    try:
        result = create_template_portal(
            new_name=name,
            description=description,
            organization=organization,
            token=token,
            username=username,
            clone_dir=clone_dir,
        )

        print(f"Successfully forked repository: {result['repository']['html_url']}")

        if result["clone_path"]:
            print(f"Cloned repository to: {result['clone_path']}")
    except Exception as e:
        logger.error(f"Error forking repository: {e}")
        sys.exit(1)


@github.command(name="configure-portal")
@click.argument(
    "repo_dir", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option(
    "--search-index",
    required=True,
    help="UUID of the Globus Search index",
)
@click.option(
    "--title",
    help="Title for the portal",
)
@click.option(
    "--subtitle",
    help="Subtitle for the portal",
)
@click.option(
    "--config-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to additional configuration JSON file",
)
@click.option(
    "--push/--no-push",
    default=False,
    help="Whether to push the changes to GitHub",
)
@click.option(
    "--repo-owner",
    help="Owner of the repository (required if --push is used)",
)
@click.option(
    "--repo-name",
    help="Name of the repository (required if --push is used)",
)
@click.option(
    "--token",
    help="GitHub personal access token (overrides config and environment)",
)
@click.option(
    "--commit-message",
    default="Configure portal",
    help="Commit message for the push",
)
@click.option(
    "--branch",
    default="main",
    help="Branch to push to",
)
@click.option(
    "--enable-pages/--no-enable-pages",
    default=False,
    help="Whether to enable GitHub Pages for the repository",
)
@click.option(
    "--pages-branch",
    default="main",
    help="Branch to publish GitHub Pages from (if --enable-pages is used)",
)
@click.option(
    "--pages-path",
    default="/",
    help="Directory to publish GitHub Pages from (if --enable-pages is used). Use '/' for root",
)
@click.option(
    "--enable-actions/--no-enable-actions",
    default=False,
    help="Whether to enable GitHub Actions for the repository",
)
def configure_portal(
    repo_dir: Path,
    search_index: str,
    title: Optional[str],
    subtitle: Optional[str],
    config_file: Optional[Path],
    push: bool,
    repo_owner: Optional[str],
    repo_name: Optional[str],
    token: Optional[str],
    commit_message: str,
    branch: str,
    enable_pages: bool,
    pages_branch: str,
    pages_path: str,
    enable_actions: bool,
):
    """
    Configure the static.json file in a Globus template search portal repository.

    REPO_DIR is the path to the repository directory.

    This command can also enable GitHub Pages and GitHub Actions for the repository
    to allow automatic publishing of the portal.
    """
    # Load additional configuration if provided
    additional_config = None
    if config_file:
        try:
            with open(config_file, "r") as f:
                additional_config = json.load(f)
        except Exception as e:
            logger.error(f"Error loading configuration file: {e}")
            sys.exit(1)

    try:
        # Validate parameters
        if (push or enable_pages or enable_actions) and (
            not repo_owner or not repo_name
        ):
            logger.error(
                "--repo-owner and --repo-name are required when --push, --enable-pages, or --enable-actions is used"
            )
            sys.exit(1)

        # Configure static.json
        static_json_path = configure_static_json(
            repo_dir=repo_dir,
            search_index=search_index,
            portal_title=title,
            portal_subtitle=subtitle,
            additional_config=additional_config,
            push_to_github=push,
            repo_owner=repo_owner,
            repo_name=repo_name,
            token=token,
            commit_message=commit_message,
            branch=branch,
        )

        if push:
            print(
                f"Successfully configured static.json at: {static_json_path} and pushed to {repo_owner}/{repo_name}"
            )
        else:
            print(f"Successfully configured static.json at: {static_json_path}")

        # Enable GitHub Pages if requested
        if enable_pages:
            client = GitHubClient(token=token)
            pages_result = client.enable_github_pages(
                repo_owner=repo_owner,
                repo_name=repo_name,
                branch=pages_branch,
                path=pages_path,
            )
            print(f"Successfully enabled GitHub Pages for {repo_owner}/{repo_name}")
            if "html_url" in pages_result:
                print(f"Site URL: {pages_result['html_url']}")

        # Enable GitHub Actions if requested
        if enable_actions:
            client = GitHubClient(token=token)
            client.enable_github_actions(
                repo_owner=repo_owner,
                repo_name=repo_name,
            )
            print(f"Successfully enabled GitHub Actions for {repo_owner}/{repo_name}")
            print(
                f"GitHub Actions workflows can now automatically publish to GitHub Pages"
            )

    except Exception as e:
        logger.error(f"Error configuring portal: {e}")
        sys.exit(1)


@cli.group()
def compute():
    """
    Globus Compute operations.
    """
    pass


@compute.command(name="remote-crawl")
@click.argument("directory", type=str)
@click.option(
    "--endpoint-id",
    required=True,
    help="Globus Compute endpoint ID",
)
@click.option(
    "--exclude",
    "-e",
    multiple=True,
    help="Glob pattern to exclude from crawling (can be used multiple times)",
)
@click.option(
    "--include",
    "-i",
    multiple=True,
    help="Glob pattern to include in crawling (can be used multiple times)",
)
@click.option(
    "--exclude-regex",
    "-E",
    multiple=True,
    help="Regex pattern to exclude from crawling (can be used multiple times)",
)
@click.option(
    "--include-regex",
    "-I",
    multiple=True,
    help="Regex pattern to include in crawling (can be used multiple times)",
)
@click.option(
    "--max-depth",
    "-d",
    type=int,
    help="Maximum depth to crawl",
)
@click.option(
    "--follow-symlinks/--no-follow-symlinks",
    default=False,
    help="Whether to follow symbolic links",
)
@click.option(
    "--polling-rate",
    "-p",
    type=float,
    help="Time in seconds to wait between file operations",
)
@click.option(
    "--ignore-dot-dirs/--include-dot-dirs",
    default=True,
    help="Whether to ignore directories starting with a dot",
)
@click.option(
    "--save-json/--no-save-json",
    default=None,
    help="Whether to save metadata as JSON files",
)
@click.option(
    "--json-dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory to save JSON metadata files in",
)
@click.option(
    "--search-index",
    help="Globus Search index UUID to publish metadata to",
)
@click.option(
    "--visible-to",
    multiple=True,
    help="Globus Auth identities that can see entries (can be used multiple times)",
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Whether to wait for the task to complete",
)
@click.option(
    "--timeout",
    type=int,
    default=3600,
    help="Timeout in seconds for waiting for the task to complete",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Path to save the metadata to",
)
def remote_crawl_cmd(
    directory: str,
    endpoint_id: str,
    exclude: List[str],
    include: List[str],
    exclude_regex: List[str],
    include_regex: List[str],
    max_depth: Optional[int],
    follow_symlinks: bool,
    polling_rate: Optional[float],
    ignore_dot_dirs: bool,
    save_json: Optional[bool],
    json_dir: Optional[Path],
    search_index: Optional[str],
    visible_to: List[str],
    wait: bool,
    timeout: int,
    output: Optional[Path],
):
    """
    Crawl a directory on a remote filesystem using Globus Compute.

    DIRECTORY is the path to the directory to crawl on the remote filesystem.
    """
    # Use command-line options or fall back to config values
    endpoint = endpoint_id or config.globus_compute_endpoint_id
    if not endpoint:
        logger.error("No Globus Compute endpoint ID provided")
        sys.exit(1)

    exclude_patterns = list(exclude) if exclude else None
    include_patterns = list(include) if include else None
    exclude_regex_patterns = list(exclude_regex) if exclude_regex else None
    include_regex_patterns = list(include_regex) if include_regex else None

    # Run remote crawl
    logger.info(f"Crawling directory {directory} on endpoint {endpoint}")

    try:
        result = remote_crawl(
            endpoint_id=endpoint,
            directory_path=directory,
            exclude_patterns=exclude_patterns,
            include_patterns=include_patterns,
            exclude_regex=exclude_regex_patterns,
            include_regex=include_regex_patterns,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            polling_rate=polling_rate,
            ignore_dot_dirs=ignore_dot_dirs,
            wait=wait,
            timeout=timeout,
            save_json=save_json,
            json_dir=json_dir,
        )

        print(result)

        if wait:
            if save_json:
                logger.info(f"Crawled {directory} and saved to {json_dir}")
            else:
                logger.info(f"Crawled {len(result)} files")

            # Save metadata to file if requested
            if output:
                with open(output, "w") as f:
                    json.dump(result, f, indent=2, default=str)
                logger.info(f"Saved metadata to {output}")

            # If save_json was true, the metadata was already saved on the remote endpoint
            # We don't need to save it again here
            if save_json:
                logger.info(
                    f"Metadata was saved to JSON directory on the remote endpoint"
                )

            # Publish metadata to Globus Search if requested
            if search_index:
                logger.info(
                    f"Publishing metadata to Globus Search index: {search_index}"
                )

                # If we have a metadata file, use remote_ingest_metadata to ingest it
                if save_json and json_dir:
                    # The metadata file path on the remote endpoint
                    metadata_file_path = str(Path(json_dir) / "SPAwn_metadata.json")

                    logger.info(f"Ingesting metadata from file: {metadata_file_path}")

                    # Use remote_ingest_metadata to ingest the metadata file

                    ingest_result = remote_ingest_metadata(
                        endpoint_id=endpoint,
                        metadata_file_path=metadata_file_path,
                        search_index=search_index,
                        visible_to=list(visible_to) if visible_to else None,
                        wait=True,
                        timeout=timeout,
                    )

                    logger.info(
                        f"Published {ingest_result.get('success', 0)} entries, failed to publish {ingest_result.get('failed', 0)} entries"
                    )
                else:
                    # We have the metadata in memory, so convert it to GMetaEntries and ingest it directly
                    from spawn.globus_search import metadata_to_gmeta_entry

                    logger.info("Converting metadata to GMetaEntries")

                    entries = []
                    for metadata in result:
                        try:
                            file_path = Path(metadata["file_path"])
                            entry = metadata_to_gmeta_entry(
                                file_path=file_path,
                                metadata=metadata,
                                visible_to=list(visible_to) if visible_to else None,
                            )
                            entries.append(entry)
                        except Exception as e:
                            logger.error(
                                f"Error converting metadata for {metadata.get('file_path')}: {e}"
                            )

                    # Create Globus Search client
                    client = GlobusSearchClient(
                        index_uuid=search_index,
                    )

                    # Ingest entries
                    ingest_result = client.ingest_entries(entries)

                    logger.info(
                        f"Published {ingest_result['success']} entries, failed to publish {ingest_result['failed']} entries"
                    )
        else:
            logger.info(f"Task ID: {result}")
            print(f"Task ID: {result}")
    except Exception as e:
        logger.error(f"Error crawling directory: {e}")
        sys.exit(1)


@compute.command(name="create-portal")
@click.option(
    "--endpoint-id",
    required=True,
    help="Globus Compute endpoint ID",
)
@click.option(
    "--name",
    required=True,
    help="Name for the forked repository",
)
@click.option(
    "--search-index",
    required=True,
    help="UUID of the Globus Search index",
)
@click.option(
    "--description",
    help="Description for the new repository",
)
@click.option(
    "--organization",
    help="Organization to create the fork in",
)
@click.option(
    "--token",
    help="GitHub personal access token (overrides config and environment)",
)
@click.option(
    "--username",
    help="GitHub username (overrides config and environment)",
)
@click.option(
    "--title",
    help="Title for the portal",
)
@click.option(
    "--subtitle",
    help="Subtitle for the portal",
)
@click.option(
    "--config-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to additional configuration JSON file",
)
@click.option(
    "--enable-pages/--no-enable-pages",
    default=False,
    help="Whether to enable GitHub Pages for the repository",
)
@click.option(
    "--enable-actions/--no-enable-actions",
    default=False,
    help="Whether to enable GitHub Actions for the repository",
)
@click.option(
    "--pages-branch",
    default="main",
    help="Branch to publish GitHub Pages from (if --enable-pages is used)",
)
@click.option(
    "--pages-path",
    default="/",
    help="Directory to publish GitHub Pages from (if --enable-pages is used). Use '/' for root",
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Whether to wait for the task to complete",
)
@click.option(
    "--timeout",
    type=int,
    default=3600,
    help="Timeout in seconds for waiting for the task to complete",
)
def create_portal_cmd(
    endpoint_id: str,
    name: str,
    search_index: str,
    description: Optional[str],
    organization: Optional[str],
    token: Optional[str],
    username: Optional[str],
    title: Optional[str],
    subtitle: Optional[str],
    config_file: Optional[Path],
    enable_pages: bool,
    enable_actions: bool,
    pages_branch: str,
    pages_path: str,
    wait: bool,
    timeout: int,
):
    """
    Create a Globus search portal remotely using Globus Compute.

    This command forks the Globus template search portal, configures it with the specified
    Globus Search index, and optionally enables GitHub Pages and GitHub Actions.
    All operations are performed remotely on a Globus Compute endpoint.
    """
    # Use command-line options or fall back to config values
    endpoint = endpoint_id or config.globus_compute_endpoint_id
    if not endpoint:
        logger.error("No Globus Compute endpoint ID provided")
        sys.exit(1)

    # Load additional configuration if provided
    additional_config = None
    if config_file:
        try:
            with open(config_file, "r") as f:
                additional_config = json.load(f)
        except Exception as e:
            logger.error(f"Error loading configuration file: {e}")
            sys.exit(1)

    # Get GitHub credentials from options or config
    github_token = token or config.github_token
    github_username = username or config.github_username

    try:
        logger.info(f"Creating portal {name} remotely on endpoint {endpoint}")

        result = create_portal_remotely(
            endpoint_id=endpoint,
            new_name=name,
            search_index=search_index,
            description=description,
            organization=organization,
            token=github_token,
            username=github_username,
            portal_title=title,
            portal_subtitle=subtitle,
            additional_config=additional_config,
            enable_pages=enable_pages,
            enable_actions=enable_actions,
            pages_branch=pages_branch,
            pages_path=pages_path,
            wait=wait,
            timeout=timeout,
        )

        if wait:
            logger.info(f"Portal creation completed")
            print(f"Repository URL: {result['repository_url']}")
            if enable_pages:
                print(f"Portal URL: {result['portal_url']}")
            print(json.dumps(result, indent=2, default=str))
        else:
            logger.info(f"Task ID: {result}")
            print(f"Task ID: {result}")

    except Exception as e:
        logger.error(f"Error creating portal: {e}")
        sys.exit(1)


@cli.group()
def flow():
    """
    Globus Flow operations.
    """
    pass


@cli.group()
def search():
    """
    Globus Search operations.
    """
    pass


@search.command(name="create-index")
@click.option(
    "--display-name",
    required=True,
    help="Display name for the search index",
)
@click.option(
    "--description",
    help="Description for the search index",
)
@click.option(
    "--visible-to",
    multiple=True,
    help="Globus Auth identities that can see this index (can be used multiple times)",
)
def create_search_index(
    display_name: str,
    description: Optional[str],
    visible_to: List[str],
):
    """
    Create a new Globus Search index.

    Creates a new search index in Globus Search that can be used for indexing metadata.
    Requires a Globus Auth token with the 'search.create_index' scope.
    """
    try:
        # Get a Globus Auth token for Search
        app = UserApp("SPAwn CLI App", client_id="367628a1-4b6a-4176-82bd-422f071d1adc")
        app.add_scope_requirements(
            {"search": [globus_sdk.scopes.SearchScopes.make_mutable("all")]}
        )
        search_client = SearchClient(app=app)

        # Set default visible_to if not provided
        visible_to_list = list(visible_to) if visible_to else ["public"]

        # Create the index
        logger.info(f"Creating Globus Search index: {display_name}")

        create_result = search_client.create_index(
            display_name=display_name,
            description=description or "",
        )

        # Print the result
        index_id = create_result["id"]
        logger.info(f"Successfully created search index: {index_id}")

        print(f"Search Index ID: {index_id}")
        print(f"Display Name: {display_name}")
        print(f"Description: {description or '(No description)'}")
        print(f"Visible To: {', '.join(visible_to_list)}")
        print(
            "\nYou can use this index ID in your configuration or with the --search-index option."
        )

    except Exception as e:
        logger.error(f"Error creating search index: {e}")
        sys.exit(1)


@flow.command(name="create")
@click.option(
    "--flow-id",
    help="Globus Flow ID. If set, the flow is updated.",
)
def create_or_update_flow_cmd(flow_id: Optional[str]):
    """
    Create a new Globus Flow for SPAwn.
    """

    try:
        flow = SPAwnFlow()

        flow_id = flow.create_or_update_flow(flow_id)

        logger.info(f"Created flow: {flow_id}")
        print(f"Flow ID: {flow_id}")
    except Exception as e:
        logger.error(f"Error creating flow: {e}")
        sys.exit(1)


@flow.command(name="run")
@click.option(
    "--flow-id",
    help="Globus Flow ID",
)
@click.option(
    "--compute-endpoint-id",
    required=True,
    help="Globus Compute endpoint ID",
)
@click.option(
    "--directory",
    required=True,
    help="Path to the directory to crawl on the remote filesystem",
)
@click.option(
    "--search-index",
    required=True,
    help="Globus Search index UUID",
)
@click.option(
    "--portal-name",
    required=True,
    help="Name for the portal repository",
)
@click.option(
    "--portal-title",
    required=True,
    help="Title for the portal",
)
@click.option(
    "--portal-subtitle",
    help="Subtitle for the portal",
)
@click.option(
    "--github-token",
    help="GitHub personal access token",
)
@click.option(
    "--github-username",
    help="GitHub username",
)
@click.option(
    "--exclude",
    "-e",
    multiple=True,
    help="Glob pattern to exclude from crawling (can be used multiple times)",
)
@click.option(
    "--include",
    "-i",
    multiple=True,
    help="Glob pattern to include in crawling (can be used multiple times)",
)
@click.option(
    "--exclude-regex",
    "-E",
    multiple=True,
    help="Regex pattern to exclude from crawling (can be used multiple times)",
)
@click.option(
    "--include-regex",
    "-I",
    multiple=True,
    help="Regex pattern to include in crawling (can be used multiple times)",
)
@click.option(
    "--max-depth",
    "-d",
    type=int,
    default=3,
    help="Maximum depth to crawl",
)
@click.option(
    "--follow-symlinks/--no-follow-symlinks",
    default=False,
    help="Whether to follow symbolic links",
)
@click.option(
    "--polling-rate",
    "-p",
    type=float,
    default=1,
    help="Time in seconds to wait between file operations",
)
@click.option(
    "--ignore-dot-dirs/--include-dot-dirs",
    default=True,
    help="Whether to ignore directories starting with a dot",
)
@click.option(
    "--visible-to",
    multiple=True,
    help="Globus Auth identities that can see entries (can be used multiple times)",
)
@click.option(
    "--wait/--no-wait",
    default=False,
    help="Whether to wait for the flow to complete",
)
@click.option(
    "--timeout",
    type=int,
    default=3600,
    help="Timeout in seconds for waiting for the flow to complete",
)
@click.option(
    "--save-json",
    default=True,
    help="Whether to save metadata as a json file",
)
@click.option(
    "--json-dir",
    type=str,
    help="Where to save json output",
)
@click.option(
    "--enable-pages",
    default=True,
    help="Whether github pages should be enabled",
)
@click.option(
    "--enable-actions",
    default=False,
    help="Whether github actions should be enabled",
)
def run_flow_cmd(
    flow_id: Optional[str],
    compute_endpoint_id: str,
    directory: str,
    search_index: str,
    portal_name: str,
    portal_title: str,
    portal_subtitle: Optional[str],
    enable_actions: bool,
    enable_pages: bool,
    github_token: Optional[str],
    github_username: Optional[str],
    exclude: List[str],
    include: List[str],
    exclude_regex: List[str],
    include_regex: List[str],
    max_depth: Optional[int],
    follow_symlinks: bool,
    polling_rate: Optional[float],
    ignore_dot_dirs: bool,
    visible_to: List[str],
    wait: bool,
    timeout: int,
    save_json: bool,
    json_dir: Optional[str],
):
    """
    Run a Globus Flow for SPAwn.
    """
    # Use command-line options or fall back to config values
    flow_id = flow_id or config.globus_flow_id
    compute_endpoint = compute_endpoint_id or config.globus_compute_endpoint_id
    if not compute_endpoint:
        logger.error("No Globus Compute endpoint ID provided")
        sys.exit(1)

    github_token = github_token or config.github_token
    github_username = github_username or config.github_username

    exclude_patterns = list(exclude) if exclude else None
    include_patterns = list(include) if include else None
    exclude_regex_patterns = list(exclude_regex) if exclude_regex else None
    include_regex_patterns = list(include_regex) if include_regex else None
    visible_to_list = list(visible_to) if visible_to else None

    try:
        # Register functions with Globus Compute
        function_ids = register_functions(compute_endpoint)
        crawl_function_id = function_ids["remote_crawl_directory"]
        portal_function_id = function_ids["remote_create_portal"]
        ingest_function_id = function_ids["ingest_metadata_from_file"]

        # Create or get flow
        flow = SPAwnFlow(
            flow_id=flow_id,
        )

        if not flow_id:
            flow_id = flow.create_flow()
            logger.info(f"Created flow: {flow_id}")

        # Run flow
        result = flow.run_flow(
            compute_endpoint_id=compute_endpoint,
            compute_crawl_function_id=crawl_function_id,
            compute_ingest_function_id=ingest_function_id,
            compute_create_portal_function_id=portal_function_id,
            directory_path=directory,
            search_index=search_index,
            portal_name=portal_name,
            portal_title=portal_title,
            portal_subtitle=portal_subtitle,
            enable_actions=enable_actions,
            enable_pages=enable_pages,
            github_token=github_token,
            github_username=github_username,
            exclude_patterns=exclude_patterns,
            include_patterns=include_patterns,
            exclude_regex=exclude_regex_patterns,
            include_regex=include_regex_patterns,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            polling_rate=polling_rate,
            ignore_dot_dirs=ignore_dot_dirs,
            visible_to=visible_to_list,
            wait=wait,
            timeout=timeout,
            save_json=save_json,
            json_dir=json_dir,
        )

        if wait:
            logger.info(f"Flow completed with status: {result['status']}")
            print(json.dumps(result, indent=2, default=str))
        else:
            logger.info(f"Flow run ID: {result}")
            print(f"Flow run ID: {result}")
    except Exception as e:
        logger.error(f"Error running flow: {e}")
        sys.exit(1)


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
