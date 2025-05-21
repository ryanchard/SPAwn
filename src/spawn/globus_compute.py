"""
Globus Compute integration for SPAwn.

This module provides functionality for remotely crawling directories using Globus Compute.
"""

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from globus_compute_sdk import Executor, Client

logger = logging.getLogger(__name__)


def remote_crawl_directory(
    directory_path: str,
    exclude_patterns: Optional[List[str]] = None,
    include_patterns: Optional[List[str]] = None,
    exclude_regex: Optional[List[str]] = None,
    include_regex: Optional[List[str]] = None,
    max_depth: Optional[int] = None,
    follow_symlinks: bool = False,
    polling_rate: Optional[float] = None,
    ignore_dot_dirs: bool = True,
    save_json: bool = False,
    json_dir: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Crawl a directory on a remote filesystem and extract metadata.

    This function is designed to be registered with Globus Compute.

    Args:
        directory_path: Path to the directory to crawl.
        exclude_patterns: Glob patterns to exclude from crawling.
        include_patterns: Glob patterns to include in crawling.
        exclude_regex: Regex patterns to exclude from crawling.
        include_regex: Regex patterns to include in crawling.
        max_depth: Maximum depth to crawl.
        follow_symlinks: Whether to follow symbolic links.
        polling_rate: Time in seconds to wait between file operations.
        ignore_dot_dirs: Whether to ignore directories starting with a dot.

    Returns:
        List of metadata dictionaries for each file.
    """
    # Import required modules
    # These imports are done here to avoid dependency issues
    # when registering the function with Globus Compute
    from pathlib import Path
    import sys
    import os
    import re
    import time
    import json

    # Add the current directory to the path to import spawn modules
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.append(current_dir)

    # Import spawn modules
    from spawn.crawler import crawl_directory
    from spawn.metadata import extract_metadata, save_metadata_to_json

    # Convert directory path to Path object
    directory = Path(directory_path)

    # Crawl directory
    files = crawl_directory(
        directory,
        exclude_patterns=exclude_patterns,
        include_patterns=include_patterns,
        exclude_regex=exclude_regex,
        include_regex=include_regex,
        max_depth=max_depth,
        follow_symlinks=follow_symlinks,
        polling_rate=polling_rate,
        ignore_dot_dirs=ignore_dot_dirs,
    )

    # Extract metadata
    metadata_dict = {}

    for file_path in files:
        try:
            metadata = extract_metadata(file_path)

            # Add file_path as string
            metadata["file_path"] = str(file_path)

            metadata_dict[str(file_path.absolute())] = metadata
        except Exception as e:
            print(f"Error extracting metadata for {file_path}: {e}")

    # Save metadata to JSON if requested
    if save_json and json_dir:
        try:
            json_dir_path = Path(json_dir)
            save_metadata_to_json(
                file_path_or_metadata=metadata_dict, output_dir=json_dir_path
            )
            print(
                f"Saved metadata for {len(metadata_dict)} files to JSON in {json_dir}"
            )
            return str(json_dir_path)
        except Exception as e:
            print(f"Error saving metadata to JSON: {e}")

    return metadata_dict


def ingest_metadata_from_file(
    metadata_file_path: str,
    search_index: str,
    visible_to: Optional[List[str]] = None,
    batch_size: int = 100,
    subject_prefix: str = "file://",
) -> Dict[str, int]:
    """
    Ingest metadata from a file into Globus Search.

    This function is designed to be registered with Globus Compute.

    Args:
        metadata_file_path: Path to the metadata file to ingest.
        search_index: UUID of the Globus Search index.
        visible_to: List of Globus Auth identities that can see these entries.
        batch_size: Number of entries to ingest in a single batch.
        subject_prefix: Prefix to use for the subject.

    Returns:
        Dictionary with counts of successful and failed ingest operations.
    """
    # Import required modules
    # These imports are done here to avoid dependency issues
    # when registering the function with Globus Compute
    import json
    import sys
    import os
    from pathlib import Path

    # Add the current directory to the path to import spawn modules
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.append(current_dir)

    # Import spawn modules
    from spawn.globus_search import publish_metadata, GlobusSearchClient

    # Load metadata from file
    try:
        with open(metadata_file_path, "r") as f:
            metadata = json.load(f)
    except Exception as e:
        print(f"Error loading metadata from {metadata_file_path}: {e}")
        return {"success": 0, "failed": 0, "error": str(e)}

    # Publish metadata to Globus Search
    try:
        result = publish_metadata(
            metadata=metadata,
            index_uuid=search_index,
            batch_size=batch_size,
            subject_prefix=subject_prefix,
            visible_to=visible_to,
        )
        return result
    except Exception as e:
        print(f"Error publishing metadata to Globus Search: {e}")
        return {"success": 0, "failed": 0, "error": str(e)}


def register_functions(endpoint_id: str) -> Dict[str, str]:
    """
    Register functions with Globus Compute.

    Args:
        endpoint_id: Globus Compute endpoint ID.

    Returns:
        Dictionary mapping function names to function IDs.
    """
    # Create Globus Compute client
    gc = Client()

    # Register functions
    function_ids = {}

    # Register remote_crawl_directory
    remote_crawl_directory_id = gc.register_function(
        remote_crawl_directory,
        function_name="remote_crawl_directory",
        description="Crawl a directory on a remote filesystem and extract metadata",
        public=False,
    )
    function_ids["remote_crawl_directory"] = remote_crawl_directory_id

    # Register ingest_metadata_from_file
    ingest_metadata_id = gc.register_function(
        ingest_metadata_from_file,
        function_name="ingest_metadata_from_file",
        description="Ingest metadata from a file into Globus Search",
        public=False,
    )
    function_ids["ingest_metadata_from_file"] = ingest_metadata_id

    # Register remote_create_portal
    remote_create_portal_id = gc.register_function(
        remote_create_portal,
        function_name="remote_create_portal",
        description="Create a Globus search portal by forking, cloning, configuring, and pushing the repository",
        public=False,
    )
    function_ids["remote_create_portal"] = remote_create_portal_id

    return function_ids


def remote_ingest_metadata(
    endpoint_id: str,
    metadata_file_path: str,
    search_index: str,
    visible_to: Optional[List[str]] = None,
    batch_size: int = 100,
    subject_prefix: str = "file://",
    wait: bool = True,
    timeout: int = 3600,
) -> Union[str, Dict[str, int]]:
    """
    Ingest metadata from a file into Globus Search using Globus Compute.

    Args:
        endpoint_id: Globus Compute endpoint ID.
        metadata_file_path: Path to the metadata file to ingest.
        search_index: UUID of the Globus Search index.
        visible_to: List of Globus Auth identities that can see these entries.
        batch_size: Number of entries to ingest in a single batch.
        subject_prefix: Prefix to use for the subject.
        wait: Whether to wait for the task to complete.
        timeout: Timeout in seconds for waiting for the task to complete.

    Returns:
        If wait is True, returns the result of the ingest operation.
        If wait is False, returns the task ID.
    """
    # Create Globus Compute client
    gce = Executor(endpoint_id=endpoint_id)

    # Submit task
    task = gce.submit(
        ingest_metadata_from_file,
        metadata_file_path=metadata_file_path,
        search_index=search_index,
        visible_to=visible_to,
        batch_size=batch_size,
        subject_prefix=subject_prefix,
    )

    logger.info(f"Submitted ingest task {task.task_id} to endpoint {endpoint_id}")

    if not wait:
        return task.task_id

    res = task.result()
    print(res)

    # Wait for task to complete
    logger.info(f"Waiting for ingest task {task.task_id} to complete...")
    result = task.result(timeout=timeout)

    logger.info(f"Ingest task {task.task_id} completed")

    return result


def remote_crawl(
    endpoint_id: str,
    directory_path: str,
    exclude_patterns: Optional[List[str]] = None,
    include_patterns: Optional[List[str]] = None,
    exclude_regex: Optional[List[str]] = None,
    include_regex: Optional[List[str]] = None,
    max_depth: Optional[int] = None,
    follow_symlinks: bool = False,
    polling_rate: Optional[float] = None,
    ignore_dot_dirs: bool = True,
    wait: bool = True,
    timeout: int = 3600,
    save_json: bool = False,
    json_dir: Optional[Path] = None,
) -> Union[str, List[Dict[str, Any]]]:
    """
    Crawl a directory on a remote filesystem using Globus Compute.

    Args:
        endpoint_id: Globus Compute endpoint ID.
        directory_path: Path to the directory to crawl.
        exclude_patterns: Glob patterns to exclude from crawling.
        include_patterns: Glob patterns to include in crawling.
        exclude_regex: Regex patterns to exclude from crawling.
        include_regex: Regex patterns to include in crawling.
        max_depth: Maximum depth to crawl.
        follow_symlinks: Whether to follow symbolic links.
        polling_rate: Time in seconds to wait between file operations.
        ignore_dot_dirs: Whether to ignore directories starting with a dot.
        wait: Whether to wait for the task to complete.
        timeout: Timeout in seconds for waiting for the task to complete.

    Returns:
        If wait is True, returns the list of metadata dictionaries.
        If wait is False, returns the task ID.
    """
    # Create Globus Compute client
    gce = Executor(endpoint_id=endpoint_id)

    # Convert json_dir to string if provided
    json_dir_str = str(json_dir) if json_dir else None

    # Submit task
    task = gce.submit(
        remote_crawl_directory,
        directory_path=directory_path,
        exclude_patterns=exclude_patterns,
        include_patterns=include_patterns,
        exclude_regex=exclude_regex,
        include_regex=include_regex,
        max_depth=max_depth,
        follow_symlinks=follow_symlinks,
        polling_rate=polling_rate,
        ignore_dot_dirs=ignore_dot_dirs,
        save_json=save_json,
        json_dir=json_dir_str,
    )

    logger.info(f"Submitted task {task.task_id} to endpoint {endpoint_id}")

    if not wait:
        return task.task_id

    # Wait for task to complete
    logger.info(f"Waiting for task {task.task_id} to complete...")
    result = task.result(timeout=timeout)

    logger.info(f"Task {task.task_id} completed with {len(result)} files processed")

    return result


def remote_create_portal(
    new_name: str,
    index_name: str,
    description: Optional[str] = None,
    organization: Optional[str] = None,
    token: Optional[str] = None,
    username: Optional[str] = None,
    portal_title: Optional[str] = None,
    portal_subtitle: Optional[str] = None,
    additional_config: Optional[Dict[str, Any]] = None,
    enable_pages: bool = True,
    enable_actions: bool = True,
    pages_branch: str = "main",
    pages_path: str = "/",
) -> Dict[str, Any]:
    """
    Create a Globus search portal by forking, cloning, configuring, and pushing the repository.

    This function is designed to be registered with Globus Compute.

    Args:
        new_name: Name for the forked repository.
        index_name: UUID of the Globus Search index.
        description: Description for the new repository.
        organization: Organization to create the fork in. If None, creates in the user's account.
        token: GitHub personal access token. If None, uses the token from config or environment.
        username: GitHub username. If None, uses the username from config or environment.
        portal_title: Title for the portal.
        portal_subtitle: Subtitle for the portal.
        additional_config: Additional configuration to add to the static.json file.
        enable_pages: Whether to enable GitHub Pages for the repository.
        enable_actions: Whether to enable GitHub Actions for the repository.
        pages_branch: Branch to publish GitHub Pages from.
        pages_path: Directory to publish GitHub Pages from. Use "/" for root.

    Returns:
        Dictionary with information about the created portal.
    """
    # Import required modules
    # These imports are done here to avoid dependency issues
    # when registering the function with Globus Compute
    import json
    import os
    import sys
    import tempfile
    from pathlib import Path

    # Add the current directory to the path to import spawn modules
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.append(current_dir)

    # Import spawn modules
    from spawn.github import fork_template_portal, configure_static_json, GitHubClient

    # Step 1: Fork the template portal
    with tempfile.TemporaryDirectory() as temp_dir:
        clone_dir = Path(temp_dir) / new_name

        # Fork and clone the repository
        fork_result = fork_template_portal(
            new_name=new_name,
            description=description,
            organization=organization,
            token=token,
            username=username,
            clone_dir=clone_dir,
        )

        # Get repository owner
        owner = organization or username
        if not owner:
            # Try to get username from the fork result
            owner = fork_result["repository"].get("owner", {}).get("login")
            if not owner:
                raise ValueError("Could not determine repository owner")

        # Step 2: Configure the portal
        static_json_path = configure_static_json(
            repo_dir=clone_dir,
            index_name=index_name,
            portal_title=portal_title,
            portal_subtitle=portal_subtitle,
            additional_config=additional_config,
            push_to_github=True,
            repo_owner=owner,
            repo_name=new_name,
            token=token,
            username=username,
            commit_message="Configure portal",
            branch="main",
        )

        # Step 3: Enable GitHub Pages and Actions if requested
        if enable_pages or enable_actions:
            client = GitHubClient(token=token, username=username)

            if enable_pages:
                pages_result = client.enable_github_pages(
                    repo_owner=owner,
                    repo_name=new_name,
                    branch=pages_branch,
                    path=pages_path,
                )

            if enable_actions:
                actions_result = client.enable_github_actions(
                    repo_owner=owner,
                    repo_name=new_name,
                )

        # Return information about the created portal
        result = {
            "repository": fork_result["repository"],
            "portal_url": f"https://{owner}.github.io/{new_name}",
            "repository_url": f"https://github.com/{owner}/{new_name}",
            "index_name": index_name,
        }

        return result


def create_portal_remotely(
    endpoint_id: str,
    new_name: str,
    index_name: str,
    description: Optional[str] = None,
    organization: Optional[str] = None,
    token: Optional[str] = None,
    username: Optional[str] = None,
    portal_title: Optional[str] = None,
    portal_subtitle: Optional[str] = None,
    additional_config: Optional[Dict[str, Any]] = None,
    enable_pages: bool = False,
    enable_actions: bool = False,
    pages_branch: str = "main",
    pages_path: str = "/",
    wait: bool = True,
    timeout: int = 3600,
) -> Union[str, Dict[str, Any]]:
    """
    Create a Globus search portal remotely using Globus Compute.

    Args:
        endpoint_id: Globus Compute endpoint ID.
        new_name: Name for the forked repository.
        index_name: UUID of the Globus Search index.
        description: Description for the new repository.
        organization: Organization to create the fork in. If None, creates in the user's account.
        token: GitHub personal access token. If None, uses the token from config or environment.
        username: GitHub username. If None, uses the username from config or environment.
        portal_title: Title for the portal.
        portal_subtitle: Subtitle for the portal.
        additional_config: Additional configuration to add to the static.json file.
        enable_pages: Whether to enable GitHub Pages for the repository.
        enable_actions: Whether to enable GitHub Actions for the repository.
        pages_branch: Branch to publish GitHub Pages from.
        pages_path: Directory to publish GitHub Pages from. Use "/" for root.
        wait: Whether to wait for the task to complete.
        timeout: Timeout in seconds for waiting for the task to complete.

    Returns:
        If wait is True, returns information about the created portal.
        If wait is False, returns the task ID.
    """
    # Create Globus Compute client
    gce = Executor(endpoint_id=endpoint_id)

    # Submit task
    task = gce.submit(
        remote_create_portal,
        new_name=new_name,
        index_name=index_name,
        description=description,
        organization=organization,
        token=token,
        username=username,
        portal_title=portal_title,
        portal_subtitle=portal_subtitle,
        additional_config=additional_config,
        enable_pages=enable_pages,
        enable_actions=enable_actions,
        pages_branch=pages_branch,
        pages_path=pages_path,
    )

    logger.info(
        f"Submitted portal creation task {task.task_id} to endpoint {endpoint_id}"
    )

    if not wait:
        return task.task_id

    # Wait for task to complete
    logger.info(f"Waiting for portal creation task {task.task_id} to complete...")
    result = task.result(timeout=timeout)

    logger.info(f"Portal creation task {task.task_id} completed")

    return result


def get_task_result(task_id: str, timeout: int = 3600) -> Any:
    """
    Get the result of a Globus Compute task.

    Args:
        task_id: Globus Compute task ID.
        timeout: Timeout in seconds for waiting for the task to complete.

    Returns:
        The result of the task.
    """
    # Create Globus Compute client
    gc = Client()

    # Get task
    task = gc.get_task(task_id)

    # Wait for task to complete
    logger.info(f"Waiting for task {task_id} to complete...")
    result = task.result(timeout=timeout)

    logger.info(f"Task {task_id} completed")

    return result
