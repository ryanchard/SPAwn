"""
Commands for Globus Flow operations.
"""

import logging
import sys
from typing import List, Optional

import click

from spawn.config import config
from spawn.globus_compute import register_functions
from spawn.globus_flow import SPAwnFlow

from spawn.cli.common import cli, logger


@cli.group()
def flow():
    """
    Globus Flow operations.
    """
    pass


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
            import json

            print(json.dumps(result, indent=2, default=str))
        else:
            logger.info(f"Flow run ID: {result}")
            print(f"Flow run ID: {result}")
    except Exception as e:
        logger.error(f"Error running flow: {e}")
        sys.exit(1)
