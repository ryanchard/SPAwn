"""
Globus Compute commands for SPAwn CLI.

Handles remote crawling and task result retrieval via Globus Compute.
"""

import logging
import sys
from pathlib import Path
from typing import List
from typing import Optional

import click

from spawn.config import config
from spawn.globus.globus_compute import get_task_result
from spawn.globus.globus_compute import remote_crawl
from spawn.globus.globus_search import GlobusSearchClient
from spawn.globus.globus_search import metadata_to_gmeta_entry

logger = logging.getLogger(__name__)


@click.group()
def compute() -> None:
    """Globus Compute operations."""
    pass


@compute.command(name="remote-crawl")
@click.argument("directory", type=str)
@click.option("--endpoint-id", required=True, help="Globus Compute endpoint ID")
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
@click.option("--max-depth", "-d", type=int, help="Maximum depth to crawl")
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
@click.option("--search-index", help="Globus Search index UUID to publish metadata to")
@click.option("--auth-token", help="Globus Auth token with search.ingest scope")
@click.option(
    "--visible-to",
    multiple=True,
    help="Globus Auth identities that can see entries (can be used multiple times)",
)
@click.option(
    "--wait/--no-wait", default=True, help="Whether to wait for the task to complete"
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
    auth_token: Optional[str] = None,
) -> None:
    """
    Crawl a directory on a remote filesystem using Globus Compute.
    """
    import json

    endpoint = endpoint_id or config.globus_compute_endpoint_id
    if not endpoint:
        logger.error("No Globus Compute endpoint ID provided")
        sys.exit(1)
    exclude_patterns = list(exclude) if exclude else None
    include_patterns = list(include) if include else None
    exclude_regex_patterns = list(exclude_regex) if exclude_regex else None
    include_regex_patterns = list(include_regex) if include_regex else None
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
        )
        if wait:
            logger.info(f"Crawled {len(result)} files")
            if output:
                with open(output, "w") as f:
                    json.dump(result, f, indent=2, default=str)
                logger.info(f"Saved metadata to {output}")
            if save_json:
                from spawn.metadata import save_metadata_to_json

                logger.info("Saving metadata to JSON files")
                json_count = 0
                for metadata in result:
                    try:
                        file_path = Path(metadata["file_path"])
                        save_metadata_to_json(file_path, metadata, json_dir)
                        json_count += 1
                    except Exception as e:
                        logger.error(
                            f"Error saving metadata for {metadata.get('file_path')}: {e}"
                        )
                logger.info(f"Saved metadata for {json_count} files to JSON")
            if search_index:
                logger.info(
                    f"Publishing metadata to Globus Search index: {search_index}"
                )
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
                client = GlobusSearchClient(
                    index_uuid=search_index,
                    auth_token=auth_token,
                )
                result = client.ingest_entries(entries)
                logger.info(
                    f"Published {result['success']} entries, failed to publish {result['failed']} entries"
                )
        else:
            logger.info(f"Task ID: {result}")
            print(f"Task ID: {result}")
    except Exception as e:
        logger.error(f"Error crawling directory: {e}")
        sys.exit(1)


@compute.command(name="get-result")
@click.argument("task_id", type=str)
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
    help="Path to save the result to",
)
def get_result_cmd(task_id: str, timeout: int, output: Optional[Path]) -> None:
    """
    Get the result of a Globus Compute task.
    """
    import json

    try:
        result = get_task_result(task_id, timeout=timeout)
        if output:
            with open(output, "w") as f:
                json.dump(result, f, indent=2, default=str)
            logger.info(f"Saved result to {output}")
        else:
            print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        logger.error(f"Error getting task result: {e}")
        sys.exit(1)
