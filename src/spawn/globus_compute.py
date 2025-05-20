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
    from spawn.metadata import extract_metadata

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
    metadata_list = []
    for file_path in files:
        try:
            metadata = extract_metadata(file_path)

            # Convert Path objects to strings for JSON serialization
            metadata["path"] = str(metadata["path"])

            # Add file_path as string
            metadata["file_path"] = str(file_path)

            metadata_list.append(metadata)
        except Exception as e:
            print(f"Error extracting metadata for {file_path}: {e}")

    return metadata_list


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

    return function_ids


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
    )

    logger.info(f"Submitted task {task.task_id} to endpoint {endpoint_id}")

    if not wait:
        return task.task_id

    # Wait for task to complete
    logger.info(f"Waiting for task {task.task_id} to complete...")
    result = task.result(timeout=timeout)

    logger.info(f"Task {task.task_id} completed with {len(result)} files processed")

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
