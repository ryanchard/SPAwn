"""
Commands for Globus Compute operations.
"""

import json
import logging
import sys
from pathlib import Path
from typing import List, Optional

import click

from spawn.config import config
from spawn.globus_compute import (
    remote_crawl,
    remote_ingest_metadata,
    get_task_result,
    create_portal_remotely,
)
from spawn.globus_search import GlobusSearchClient, metadata_to_gmeta_entry

from spawn.cli.common import cli, logger


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
