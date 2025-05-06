"""
Globus Flow integration for SPAwn.

This module provides functionality for creating and running Globus Flows
to orchestrate the entire process of crawling, indexing, and portal creation.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Import these only when needed to avoid dependency issues
GLOBUS_FLOW_IMPORTS = False

logger = logging.getLogger(__name__)


def _ensure_globus_flow_imports():
    """Ensure Globus Flow imports are available."""
    global GLOBUS_FLOW_IMPORTS
    if not GLOBUS_FLOW_IMPORTS:
        try:
            global globus_automate_client
            import globus_automate_client
            GLOBUS_FLOW_IMPORTS = True
        except ImportError:
            logger.error("Globus Automate Client not installed. Run 'pip install globus-automate-client'")
            raise


class SPAwnFlow:
    """Class for creating and running Globus Flows for SPAwn."""

    FLOW_DEFINITION = {
        "title": "SPAwn Flow",
        "description": "A flow to crawl a directory, publish metadata to Globus Search, and create a portal",
        "input_schema": {
            "type": "object",
            "properties": {
                "compute_endpoint_id": {
                    "type": "string",
                    "description": "Globus Compute endpoint ID"
                },
                "directory_path": {
                    "type": "string",
                    "description": "Path to the directory to crawl"
                },
                "search_index": {
                    "type": "string",
                    "description": "Globus Search index UUID"
                },
                "portal_name": {
                    "type": "string",
                    "description": "Name for the portal repository"
                },
                "portal_title": {
                    "type": "string",
                    "description": "Title for the portal"
                },
                "portal_subtitle": {
                    "type": "string",
                    "description": "Subtitle for the portal"
                },
                "github_token": {
                    "type": "string",
                    "description": "GitHub personal access token"
                },
                "github_username": {
                    "type": "string",
                    "description": "GitHub username"
                },
                "exclude_patterns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Glob patterns to exclude from crawling"
                },
                "include_patterns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Glob patterns to include in crawling"
                },
                "exclude_regex": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Regex patterns to exclude from crawling"
                },
                "include_regex": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Regex patterns to include in crawling"
                },
                "max_depth": {
                    "type": "integer",
                    "description": "Maximum depth to crawl"
                },
                "follow_symlinks": {
                    "type": "boolean",
                    "description": "Whether to follow symbolic links"
                },
                "polling_rate": {
                    "type": "number",
                    "description": "Time in seconds to wait between file operations"
                },
                "ignore_dot_dirs": {
                    "type": "boolean",
                    "description": "Whether to ignore directories starting with a dot"
                },
                "visible_to": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Globus Auth identities that can see entries"
                }
            },
            "required": [
                "compute_endpoint_id",
                "directory_path",
                "search_index",
                "portal_name",
                "portal_title"
            ]
        },
        "definition": {
            "StartAt": "CrawlDirectory",
            "States": {
                "CrawlDirectory": {
                    "Type": "Action",
                    "ActionUrl": "https://compute.actions.globus.org",
                    "Parameters": {
                        "endpoint.$": "$.compute_endpoint_id",
                        "function.$": "$.compute_function_id",
                        "kwargs": {
                            "directory_path.$": "$.directory_path",
                            "exclude_patterns.$": "$.exclude_patterns",
                            "include_patterns.$": "$.include_patterns",
                            "exclude_regex.$": "$.exclude_regex",
                            "include_regex.$": "$.include_regex",
                            "max_depth.$": "$.max_depth",
                            "follow_symlinks.$": "$.follow_symlinks",
                            "polling_rate.$": "$.polling_rate",
                            "ignore_dot_dirs.$": "$.ignore_dot_dirs"
                        }
                    },
                    "ResultPath": "$.crawl_result",
                    "Next": "PublishToSearch"
                },
                "PublishToSearch": {
                    "Type": "Action",
                    "ActionUrl": "https://actions.globus.org/search/ingest",
                    "Parameters": {
                        "search_index.$": "$.search_index",
                        "visible_to.$": "$.visible_to",
                        "entries.$": "$.crawl_result.details.result"
                    },
                    "ResultPath": "$.search_result",
                    "Next": "CreatePortal"
                },
                "CreatePortal": {
                    "Type": "Action",
                    "ActionUrl": "https://actions.globus.org/github/fork",
                    "Parameters": {
                        "repo_owner": "globus",
                        "repo_name": "template-search-portal",
                        "new_name.$": "$.portal_name",
                        "token.$": "$.github_token",
                        "username.$": "$.github_username"
                    },
                    "ResultPath": "$.fork_result",
                    "Next": "ConfigurePortal"
                },
                "ConfigurePortal": {
                    "Type": "Action",
                    "ActionUrl": "https://actions.globus.org/github/update_file",
                    "Parameters": {
                        "repo_owner.$": "$.github_username",
                        "repo_name.$": "$.portal_name",
                        "file_path": "static.json",
                        "content": {
                            "index": {
                                "uuid.$": "$.search_index",
                                "name.$": "$.search_index"
                            },
                            "branding": {
                                "title.$": "$.portal_title",
                                "subtitle.$": "$.portal_subtitle"
                            }
                        },
                        "message": "Configure portal",
                        "token.$": "$.github_token"
                    },
                    "ResultPath": "$.configure_result",
                    "End": true
                }
            }
        }
    }

    def __init__(
        self,
        flow_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        flow_scope: Optional[str] = None,
    ):
        """
        Initialize the SPAwn Flow.

        Args:
            flow_id: Existing Globus Flow ID. If None, a new flow will be created.
            client_id: Globus Auth client ID.
            client_secret: Globus Auth client secret.
            flow_scope: Globus Auth scope for the flow.
        """
        self.flow_id = flow_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.flow_scope = flow_scope
        self.flow_client = None

    def _get_flow_client(self):
        """Get the Globus Automate Client."""
        _ensure_globus_flow_imports()

        if self.flow_client is None:
            self.flow_client = globus_automate_client.FlowsClient(
                client_id=self.client_id,
                client_secret=self.client_secret,
            )

        return self.flow_client

    def create_flow(self) -> str:
        """
        Create a new Globus Flow.

        Returns:
            The ID of the created flow.
        """
        flow_client = self._get_flow_client()

        # Create flow
        flow = flow_client.create_flow(
            self.FLOW_DEFINITION["title"],
            self.FLOW_DEFINITION["definition"],
            self.FLOW_DEFINITION["input_schema"],
            description=self.FLOW_DEFINITION["description"],
            visible_to=["public"],
        )

        self.flow_id = flow["id"]
        logger.info(f"Created flow: {self.flow_id}")

        return self.flow_id

    def run_flow(
        self,
        compute_endpoint_id: str,
        compute_function_id: str,
        directory_path: str,
        search_index: str,
        portal_name: str,
        portal_title: str,
        portal_subtitle: Optional[str] = None,
        github_token: Optional[str] = None,
        github_username: Optional[str] = None,
        exclude_patterns: Optional[List[str]] = None,
        include_patterns: Optional[List[str]] = None,
        exclude_regex: Optional[List[str]] = None,
        include_regex: Optional[List[str]] = None,
        max_depth: Optional[int] = None,
        follow_symlinks: bool = False,
        polling_rate: Optional[float] = None,
        ignore_dot_dirs: bool = True,
        visible_to: Optional[List[str]] = None,
        label: Optional[str] = None,
        wait: bool = False,
        timeout: int = 3600,
    ) -> Union[str, Dict[str, Any]]:
        """
        Run the SPAwn Flow.

        Args:
            compute_endpoint_id: Globus Compute endpoint ID.
            compute_function_id: Globus Compute function ID for remote_crawl_directory.
            directory_path: Path to the directory to crawl.
            search_index: Globus Search index UUID.
            portal_name: Name for the portal repository.
            portal_title: Title for the portal.
            portal_subtitle: Subtitle for the portal.
            github_token: GitHub personal access token.
            github_username: GitHub username.
            exclude_patterns: Glob patterns to exclude from crawling.
            include_patterns: Glob patterns to include in crawling.
            exclude_regex: Regex patterns to exclude from crawling.
            include_regex: Regex patterns to include in crawling.
            max_depth: Maximum depth to crawl.
            follow_symlinks: Whether to follow symbolic links.
            polling_rate: Time in seconds to wait between file operations.
            ignore_dot_dirs: Whether to ignore directories starting with a dot.
            visible_to: Globus Auth identities that can see entries.
            label: Label for the flow run.
            wait: Whether to wait for the flow to complete.
            timeout: Timeout in seconds for waiting for the flow to complete.

        Returns:
            If wait is True, returns the flow result.
            If wait is False, returns the flow run ID.
        """
        if self.flow_id is None:
            self.create_flow()

        flow_client = self._get_flow_client()

        # Prepare flow input
        flow_input = {
            "compute_endpoint_id": compute_endpoint_id,
            "compute_function_id": compute_function_id,
            "directory_path": directory_path,
            "search_index": search_index,
            "portal_name": portal_name,
            "portal_title": portal_title,
        }

        # Add optional parameters
        if portal_subtitle is not None:
            flow_input["portal_subtitle"] = portal_subtitle
        if github_token is not None:
            flow_input["github_token"] = github_token
        if github_username is not None:
            flow_input["github_username"] = github_username
        if exclude_patterns is not None:
            flow_input["exclude_patterns"] = exclude_patterns
        if include_patterns is not None:
            flow_input["include_patterns"] = include_patterns
        if exclude_regex is not None:
            flow_input["exclude_regex"] = exclude_regex
        if include_regex is not None:
            flow_input["include_regex"] = include_regex
        if max_depth is not None:
            flow_input["max_depth"] = max_depth
        if follow_symlinks is not None:
            flow_input["follow_symlinks"] = follow_symlinks
        if polling_rate is not None:
            flow_input["polling_rate"] = polling_rate
        if ignore_dot_dirs is not None:
            flow_input["ignore_dot_dirs"] = ignore_dot_dirs
        if visible_to is not None:
            flow_input["visible_to"] = visible_to
        else:
            flow_input["visible_to"] = ["public"]

        # Run flow
        flow_run = flow_client.run_flow(
            self.flow_id,
            flow_input,
            label=label or f"SPAwn Flow: {portal_name}",
            tags=["spawn"],
        )

        flow_run_id = flow_run["run_id"]
        logger.info(f"Started flow run: {flow_run_id}")

        if not wait:
            return flow_run_id

        # Wait for flow to complete
        logger.info(f"Waiting for flow run {flow_run_id} to complete...")
        start_time = time.time()
        while True:
            status = flow_client.get_run(flow_run_id)
            if status["status"] in ["SUCCEEDED", "FAILED"]:
                break
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Flow run {flow_run_id} timed out")
            time.sleep(10)

        logger.info(f"Flow run {flow_run_id} completed with status: {status['status']}")

        return status


def create_and_run_flow(
    compute_endpoint_id: str,
    directory_path: str,
    search_index: str,
    portal_name: str,
    portal_title: str,
    portal_subtitle: Optional[str] = None,
    github_token: Optional[str] = None,
    github_username: Optional[str] = None,
    exclude_patterns: Optional[List[str]] = None,
    include_patterns: Optional[List[str]] = None,
    exclude_regex: Optional[List[str]] = None,
    include_regex: Optional[List[str]] = None,
    max_depth: Optional[int] = None,
    follow_symlinks: bool = False,
    polling_rate: Optional[float] = None,
    ignore_dot_dirs: bool = True,
    visible_to: Optional[List[str]] = None,
    wait: bool = False,
    timeout: int = 3600,
) -> Union[str, Dict[str, Any]]:
    """
    Create and run a SPAwn Flow.

    Args:
        compute_endpoint_id: Globus Compute endpoint ID.
        directory_path: Path to the directory to crawl.
        search_index: Globus Search index UUID.
        portal_name: Name for the portal repository.
        portal_title: Title for the portal.
        portal_subtitle: Subtitle for the portal.
        github_token: GitHub personal access token.
        github_username: GitHub username.
        exclude_patterns: Glob patterns to exclude from crawling.
        include_patterns: Glob patterns to include in crawling.
        exclude_regex: Regex patterns to exclude from crawling.
        include_regex: Regex patterns to include in crawling.
        max_depth: Maximum depth to crawl.
        follow_symlinks: Whether to follow symbolic links.
        polling_rate: Time in seconds to wait between file operations.
        ignore_dot_dirs: Whether to ignore directories starting with a dot.
        visible_to: Globus Auth identities that can see entries.
        wait: Whether to wait for the flow to complete.
        timeout: Timeout in seconds for waiting for the flow to complete.

    Returns:
        If wait is True, returns the flow result.
        If wait is False, returns the flow run ID.
    """
    # Import here to avoid circular imports
    from spawn.globus_compute import register_functions

    # Register functions with Globus Compute
    function_ids = register_functions(compute_endpoint_id)
    compute_function_id = function_ids["remote_crawl_directory"]

    # Create and run flow
    flow = SPAwnFlow()
    return flow.run_flow(
        compute_endpoint_id=compute_endpoint_id,
        compute_function_id=compute_function_id,
        directory_path=directory_path,
        search_index=search_index,
        portal_name=portal_name,
        portal_title=portal_title,
        portal_subtitle=portal_subtitle,
        github_token=github_token,
        github_username=github_username,
        exclude_patterns=exclude_patterns,
        include_patterns=include_patterns,
        exclude_regex=exclude_regex,
        include_regex=include_regex,
        max_depth=max_depth,
        follow_symlinks=follow_symlinks,
        polling_rate=polling_rate,
        ignore_dot_dirs=ignore_dot_dirs,
        visible_to=visible_to,
        wait=wait,
        timeout=timeout,
    )