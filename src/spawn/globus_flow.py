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

import globus_sdk

from globus_sdk import UserApp, FlowsClient

logger = logging.getLogger(__name__)


class SPAwnFlow:
    """Class for creating and running Globus Flows for SPAwn."""

    FLOW_DEFINITION = {
        "title": "SPAwn Flow",
        "description": "A flow to crawl a directory, publish metadata to Globus Search, and create a portal",
        "input_schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "SPAwn Flow Input Schema",
            "description": "Input schema for the SPAwn Flow that crawls a directory, publishes metadata to Globus Search, and creates a portal",
            "type": "object",
            "required": [
                "compute_endpoint_id",
                "compute_crawl_function_id",
                "compute_ingest_function_id",
                "compute_create_portal_function_id",
                "directory_path",
                "search_index",
                "portal_name",
                "portal_title",
            ],
            "properties": {
                "compute_endpoint_id": {
                    "type": "string",
                    "description": "Globus Compute endpoint ID",
                    "default": "9fbce794-993e-47a7-85d0-10b04b18d9db",
                },
                "compute_crawl_function_id": {
                    "type": "string",
                    "description": "Globus Compute function ID for remote_crawl_directory",
                    "default": "68e33da5-7be4-4252-8814-bd3a487468e1",
                },
                "compute_ingest_function_id": {
                    "type": "string",
                    "description": "Globus Compute function ID for remote_ingest_metadata_from_file",
                    "default": "f7e36449-2466-41b3-accc-9b8bfab80c00",
                },
                "compute_create_portal_function_id": {
                    "type": "string",
                    "description": "Globus Compute function ID for remote_create_portal",
                    "default": "4f9c82e6-54fe-4625-ac49-eeeeea48d132",
                },
                "directory_path": {
                    "type": "string",
                    "description": "Path to the directory to crawl",
                    "default": "/Users/ryan/src/test/",
                },
                "search_index": {
                    "type": "string",
                    "description": "Globus Search index UUID",
                    "default": "e263b547-bdff-4d7a-83f0-f50a05f97771",
                },
                "portal_name": {
                    "type": "string",
                    "description": "Name for the portal repository",
                    "default": "SPAwned-1",
                },
                "portal_title": {
                    "type": "string",
                    "description": "Title for the portal",
                    "default": "spawn-portal",
                },
                "portal_subtitle": {
                    "type": "string",
                    "description": "Subtitle for the portal",
                    "default": "A search portal",
                },
                "enable_pages": {
                    "type": "boolean",
                    "description": "Whether GitHub Pages should be enabled on the portal",
                    "default": True,
                },
                "enable_actions": {
                    "type": "boolean",
                    "description": "Whether GitHub Actions should be enabled on the portal",
                    "default": True,
                },
                "github_token": {
                    "type": "string",
                    "description": "GitHub personal access token",
                },
                "github_username": {
                    "type": "string",
                    "description": "GitHub username",
                    "default": "ryanchard",
                },
                # "exclude_patterns": {
                #     "type": "string",
                #     "description": "Glob patterns to exclude from crawling",
                #     "default": "",
                # },
                # "include_patterns": {
                #     "type": "string",
                #     "description": "Glob patterns to include in crawling",
                #     "default": "",
                # },
                # "exclude_regex": {
                #     "type": "string",
                #     "description": "Regex patterns to exclude from crawling",
                #     "default": "",
                # },
                # "include_regex": {
                #     "type": "string",
                #     "description": "Regex patterns to include in crawling",
                #     "default": "",
                # },
                "max_depth": {
                    "type": "integer",
                    "description": "Maximum depth to crawl",
                    "default": 3,
                },
                "batch_size": {
                    "type": "integer",
                    "description": "The size of batches to ingest to Search",
                    "default": 100,
                },
                "follow_symlinks": {
                    "type": "boolean",
                    "description": "Whether to follow symbolic links",
                    "default": False,
                },
                "polling_rate": {
                    "type": "number",
                    "description": "Time in seconds to wait between file operations",
                    "default": 1,
                },
                "ignore_dot_dirs": {
                    "type": "boolean",
                    "description": "Whether to ignore directories starting with a dot",
                    "default": True,
                },
                "visible_to": {
                    "type": "array",
                    "description": "Globus Auth identities that can see entries",
                    "items": {"type": "string"},
                    "default": ["public"],
                },
                "save_json": {
                    "type": "boolean",
                    "description": "Whether to save metadata to a JSON file",
                    "default": True,
                },
                "json_dir": {
                    "type": "string",
                    "description": "Where to save JSON output",
                    "default": "/tmp/spawn_test/flow/",
                },
            },
        },
        "definition": {
            "StartAt": "CrawlDirectory",
            "States": {
                "CrawlDirectory": {
                    "Type": "Action",
                    "ActionUrl": "https://compute.actions.globus.org",
                    "Parameters": {
                        "endpoint.$": "$.compute_endpoint_id",
                        "function.$": "$.compute_crawl_function_id",
                        "kwargs": {
                            "directory_path.$": "$.directory_path",
                            # "exclude_patterns.$": "$.exclude_patterns",
                            # "include_patterns.$": "$.include_patterns",
                            # "exclude_regex.$": "$.exclude_regex",
                            # "include_regex.$": "$.include_regex",
                            "max_depth.$": "$.max_depth",
                            "follow_symlinks.$": "$.follow_symlinks",
                            "polling_rate.$": "$.polling_rate",
                            "ignore_dot_dirs.$": "$.ignore_dot_dirs",
                            "save_json.$": "$.save_json",
                            "json_dir.$": "$.json_dir",
                        },
                    },
                    "ResultPath": "$.crawl_result",
                    "Next": "PublishToSearch",
                },
                "PublishToSearch": {
                    "Type": "Action",
                    "ActionUrl": "https://compute.actions.globus.org",
                    "Parameters": {
                        "endpoint.$": "$.compute_endpoint_id",
                        "function.$": "$.compute_ingest_function_id",
                        "kwargs": {
                            "search_index.$": "$.search_index",
                            "visible_to.$": "$.visible_to",
                            "batch_size.$": "$.batch_size",
                            "metadata_file_path.$": "$.crawl_result.details.result[0]",
                        },
                    },
                    "ResultPath": "$.search_result",
                    "Next": "CreatePortal",
                },
                "CreatePortal": {
                    "Type": "Action",
                    "ActionUrl": "https://compute.actions.globus.org",
                    "Parameters": {
                        "endpoint.$": "$.compute_endpoint_id",
                        "function.$": "$.compute_create_portal_function_id",
                        "kwargs": {
                            "new_name.$": "$.portal_name",
                            "search_index.$": "$.search_index",
                            "token.$": "$.github_token",
                            "username.$": "$.github_username",
                            "portal_title.$": "$.portal_title",
                            "portal_subtitle.$": "$.portal_subtitle",
                            "enable_pages.$": "$.enable_pages",
                            "enable_actions.$": "$.enable_actions",
                        },
                    },
                    "ResultPath": "$.portal_result",
                    "End": True,
                },
            },
        },
    }

    def __init__(self, flow_id: Optional[str] = None):
        """
        Initialize the SPAwn Flow.

        Args:
            flow_id: Existing Globus Flow ID. If None, a new flow will be created.
            flow_scope: Globus Auth scope for the flow.
        """
        self.flow_client = None
        self.specific_flow_client = None
        self.flow_id = flow_id

    def _get_flow_client(self):
        """Get the Globus Flows Client."""

        if self.flow_client is None:
            app = UserApp(
                "SPAwn CLI App", client_id="367628a1-4b6a-4176-82bd-422f071d1adc"
            )
            self.flow_client = globus_sdk.FlowsClient(app=app)

        return self.flow_client

    def _get_specific_flow_client(self, flow_id: str):
        """Get the Globus Flows Specific Client."""

        if self.specific_flow_client is None:
            app = UserApp(
                "SPAwn CLI App", client_id="367628a1-4b6a-4176-82bd-422f071d1adc"
            )
            self.specific_flow_client = globus_sdk.SpecificFlowClient(
                app=app, flow_id=flow_id
            )

        return self.specific_flow_client

    def create_or_update_flow(self, flow_id: Optional[str] = None) -> str:
        """
        Create a new Globus Flow.

        Args:
            flow_id: The flow ID. If set, update the flow.
        Returns:
            The ID of the created flow.
        """
        flow_client = self._get_flow_client()

        if flow_id:
            flow = flow_client.update_flow(
                flow_id=flow_id,
                title=self.FLOW_DEFINITION["title"],
                definition=self.FLOW_DEFINITION["definition"],
                input_schema=self.FLOW_DEFINITION["input_schema"],
                description=self.FLOW_DEFINITION["description"],
            )
            logger.info(f"Updated flow: {flow_id}")
            return flow_id
        else:
            # Create flow
            flow = flow_client.create_flow(
                self.FLOW_DEFINITION["title"],
                self.FLOW_DEFINITION["definition"],
                self.FLOW_DEFINITION["input_schema"],
                description=self.FLOW_DEFINITION["description"],
            )
            self.flow_id = flow["id"]
            logger.info(f"Created flow: {self.flow_id}")

        return self.flow_id

    def run_flow(
        self,
        compute_endpoint_id: str,
        compute_crawl_function_id: str,
        compute_ingest_function_id: str,
        compute_create_portal_function_id: str,
        directory_path: str,
        search_index: str,
        portal_name: str,
        portal_title: str,
        portal_subtitle: Optional[str] = None,
        enable_pages: bool = True,
        enable_actions: bool = True,
        github_token: Optional[str] = None,
        github_username: Optional[str] = None,
        exclude_patterns: Optional[List[str]] = [],
        include_patterns: Optional[List[str]] = [],
        exclude_regex: Optional[List[str]] = [],
        include_regex: Optional[List[str]] = [],
        max_depth: Optional[int] = 3,
        batch_size: Optional[int] = 100,
        follow_symlinks: bool = False,
        polling_rate: Optional[float] = 1,
        ignore_dot_dirs: bool = True,
        visible_to: Optional[List[str]] = None,
        label: Optional[str] = None,
        wait: bool = False,
        timeout: int = 3600,
        save_json: bool = False,
        json_dir: Optional[str] = None,
    ) -> Union[str, Dict[str, Any]]:
        """
        Run the SPAwn Flow.

        Args:
            compute_endpoint_id: Globus Compute endpoint ID.
            compute_crawl_function_id: Globus Compute function ID for remote_crawl_directory.
            compute_ingest_function_id: Globus Compute function ID for remote_ingest_metadata_from_file.
            compute_create_portal_function_id: Globus Compute function ID for remote_create_portal.
            directory_path: Path to the directory to crawl.
            search_index: Globus Search index UUID.
            portal_name: Name for the portal repository.
            portal_title: Title for the portal.
            portal_subtitle: Subtitle for the portal.
            enable_actions: Whether github actions should be enabled on the portal.
            enable_pages: Whether github pages should be enabled on the portal.
            github_token: GitHub personal access token.
            github_username: GitHub username.
            exclude_patterns: Glob patterns to exclude from crawling.
            include_patterns: Glob patterns to include in crawling.
            exclude_regex: Regex patterns to exclude from crawling.
            include_regex: Regex patterns to include in crawling.
            max_depth: Maximum depth to crawl.
            batch_size: The size of batches to ingest to Search
            follow_symlinks: Whether to follow symbolic links.
            polling_rate: Time in seconds to wait between file operations.
            ignore_dot_dirs: Whether to ignore directories starting with a dot.
            visible_to: Globus Auth identities that can see entries.
            label: Label for the flow run.
            wait: Whether to wait for the flow to complete.
            timeout: Timeout in seconds for waiting for the flow to complete.
            save_json: Whether to save metadata to a json file
            json_dir: Where to save json output

        Returns:
            If wait is True, returns the flow result.
            If wait is False, returns the flow run ID.
        """
        if self.flow_id is None:
            self.create_flow()

        flow_client = self._get_flow_client()
        specific_flow_client = self._get_specific_flow_client(self.flow_id)

        # Prepare flow input
        flow_input = {
            "compute_endpoint_id": compute_endpoint_id,
            "compute_crawl_function_id": compute_crawl_function_id,
            "compute_ingest_function_id": compute_ingest_function_id,
            "compute_create_portal_function_id": compute_create_portal_function_id,
            "directory_path": directory_path,
            "search_index": search_index,
            "portal_name": portal_name,
            "portal_title": portal_title,
            "enable_pages": enable_pages,
            "enable_actions": enable_actions,
            # "exclude_patterns": exclude_patterns,
            # "include_patterns": include_patterns,
            # "exclude_regex": exclude_regex,
            # "include_regex": include_regex,
            "max_depth": max_depth,
            "batch_size": batch_size,
            "ignore_dot_dirs": ignore_dot_dirs,
            "save_json": save_json,
            "json_dir": json_dir,
            "polling_rate": polling_rate,
            "follow_symlinks": follow_symlinks,
        }

        # Add optional parameters
        if portal_subtitle is not None:
            flow_input["portal_subtitle"] = portal_subtitle
        if github_token is not None:
            flow_input["github_token"] = github_token
        if github_username is not None:
            flow_input["github_username"] = github_username
        if visible_to is not None:
            flow_input["visible_to"] = visible_to
        else:
            flow_input["visible_to"] = ["public"]

        # Run flow
        flow_run = specific_flow_client.run_flow(
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
    max_depth: Optional[int] = 3,
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
    compute_crawl_function_id = function_ids["remote_crawl_directory"]
    compute_ingest_function_id = function_ids["ingest_metadata_from_file"]
    compute_create_portal_function_id = function_ids["remote_create_portal"]

    # Create and run flow
    flow = SPAwnFlow()
    return flow.run_flow(
        compute_endpoint_id=compute_endpoint_id,
        compute_crawl_function_id=compute_crawl_function_id,
        compute_ingest_function_id=compute_ingest_function_id,
        compute_create_portal_function_id=compute_create_portal_function_id,
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
