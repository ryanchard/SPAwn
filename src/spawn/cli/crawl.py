"""
Crawl command for SPAwn CLI.

Handles directory crawling and indexing files.
"""
from typing import List, Optional
from pathlib import Path
import logging
import sys
import click
from spawn.config import config
from spawn.crawler import crawl_directory
from spawn.globus.globus_search import publish_metadata, GlobusSearchClient

logger = logging.getLogger(__name__)

@click.command()
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
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
) -> None:
    """
    Crawl a directory and index discovered files.

    DIRECTORY is the path to the directory to crawl.
    """
    logger.info(f"Crawling directory: {directory}")
    exclude_patterns = list(exclude) if exclude else None
    include_patterns = list(include) if include else None
    exclude_regex_patterns = list(exclude_regex) if exclude_regex else None
    include_regex_patterns = list(include_regex) if include_regex else None
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
        for file in files:
            print(file)
        return
    metadata = {}
    if save_json:
        from spawn.extractors.metadata import extract_metadata, save_metadata_to_json
        logger.info("Saving metadata to JSON files")
        json_count = 0
        for file_path in files:
            try:
                metadata[str(file_path.absolute())] = extract_metadata(file_path)
                json_count += 1
            except Exception as e:
                logger.error(f"Error saving metadata for {file_path}: {e}")
        save_metadata_to_json(metadata, json_dir)
        logger.info(f"Saved metadata for {json_count} files to JSON")
    index_uuid = search_index or config.globus_search_index
    if not index_uuid:
        logger.error("No Globus Search index UUID provided")
        return
    app = None
    try:
        import globus_sdk
        app = globus_sdk.UserApp("SPAwn CLI App", client_id="367628a1-4b6a-4176-82bd-422f071d1adc")
        app.add_scope_requirements({'search': [globus_sdk.scopes.SearchScopes.make_mutable("all")]})
        search_client = globus_sdk.SearchClient(app=app)
    except Exception as e:
        logger.error(f"Error initializing Globus SDK: {e}")
        return
    visible_to_list = list(visible_to) if visible_to else config.globus_search_visible_to
    result = publish_metadata(
        metadata=metadata,
        index_uuid=index_uuid,
        search_client=search_client,
        visible_to=visible_to_list,
    )
    logger.info(f"Published {result['success']} entries, failed to publish {result['failed']} entries") 