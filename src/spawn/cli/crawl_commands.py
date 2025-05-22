"""
Commands for crawling directories and extracting metadata.
"""

import json
import logging
from pathlib import Path
from typing import List, Optional

import click

from spawn.config import config
from spawn.crawler import crawl_directory
from spawn.globus_search import publish_metadata, GlobusSearchClient
from spawn.metadata import extract_metadata, save_metadata_to_json

from spawn.cli.common import cli, logger


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
        import sys

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
        import sys

        sys.exit(1)

    client = GlobusSearchClient(index_uuid=index_uuid)

    if subject:
        # Get entry by subject
        entry = client.get_entry(index_uuid, subject)

        if entry:
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
