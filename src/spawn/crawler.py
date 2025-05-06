"""
Directory crawler for SPAwn.

This module provides functionality for crawling directories and discovering files.
"""

import os
import logging
import re
import time
from pathlib import Path
from typing import Dict, Generator, List, Optional, Set, Tuple, Any, Pattern, Union

from tqdm import tqdm

from spawn.config import config

logger = logging.getLogger(__name__)


class Crawler:
    """Directory crawler for discovering files."""

    def __init__(
        self,
        root_dir: Path,
        exclude_patterns: Optional[List[str]] = None,
        include_patterns: Optional[List[str]] = None,
        exclude_regex: Optional[List[str]] = None,
        include_regex: Optional[List[str]] = None,
        max_depth: Optional[int] = None,
        follow_symlinks: bool = False,
        polling_rate: Optional[float] = None,
        ignore_dot_dirs: bool = True,
    ):
        """
        Initialize the crawler.

        Args:
            root_dir: The root directory to crawl.
            exclude_patterns: Glob patterns to exclude from crawling (e.g., "*.tmp").
            include_patterns: Glob patterns to include in crawling (e.g., "*.txt").
            exclude_regex: Regex patterns to exclude from crawling (e.g., r"^\..*$" for hidden files).
            include_regex: Regex patterns to include in crawling (e.g., r".*\.csv$" for CSV files).
            max_depth: Maximum depth to crawl.
            follow_symlinks: Whether to follow symbolic links.
            polling_rate: Time in seconds to wait between file operations.
            ignore_dot_dirs: Whether to ignore directories starting with a dot (default: True).
        """
        self.root_dir = Path(root_dir).expanduser().absolute()
        self.exclude_patterns = exclude_patterns or []
        self.include_patterns = include_patterns or ["*"]
        
        # Add regex for dot directories if requested
        exclude_regex_list = exclude_regex or []
        if ignore_dot_dirs:
            # Add pattern to exclude directories starting with a dot
            exclude_regex_list.append(r"/\.[^/]*(/|$)")
        
        self.exclude_regex = [re.compile(pattern) for pattern in exclude_regex_list]
        self.include_regex = [re.compile(pattern) for pattern in (include_regex or [])] or [re.compile(r".*")]
        self.max_depth = max_depth
        self.follow_symlinks = follow_symlinks
        self.polling_rate = polling_rate if polling_rate is not None else config.crawler_polling_rate
        self.visited_dirs: Set[Path] = set()

    def crawl(self) -> Generator[Path, None, None]:
        """
        Crawl the directory and yield discovered files.

        Yields:
            Paths to discovered files.
        """
        if not self.root_dir.exists():
            logger.error(f"Root directory does not exist: {self.root_dir}")
            return

        if not self.root_dir.is_dir():
            logger.error(f"Root path is not a directory: {self.root_dir}")
            return

        logger.info(f"Starting crawl of {self.root_dir}")
        
        # Get total number of files for progress bar
        total_files = sum(1 for _ in self.root_dir.rglob("*") if _.is_file())
        
        with tqdm(total=total_files, desc="Crawling") as pbar:
            for path in self._crawl_directory(self.root_dir, depth=0):
                yield path
                pbar.update(1)

    def _crawl_directory(
        self, directory: Path, depth: int = 0
    ) -> Generator[Path, None, None]:
        """
        Recursively crawl a directory.

        Args:
            directory: The directory to crawl.
            depth: The current depth.

        Yields:
            Paths to discovered files.
        """
        # Check max depth
        if self.max_depth is not None and depth > self.max_depth:
            return

        # Avoid cycles with symlinks
        if directory in self.visited_dirs:
            return
        self.visited_dirs.add(directory)

        try:
            for path in directory.iterdir():
                path_str = str(path)
                
                # Apply polling rate if configured
                if self.polling_rate > 0:
                    time.sleep(self.polling_rate)
                
                # Skip if excluded by glob patterns
                if any(path.match(pattern) for pattern in self.exclude_patterns):
                    logger.debug(f"Skipping excluded path (glob): {path}")
                    continue
                
                # Skip if excluded by regex patterns
                if any(pattern.search(path_str) for pattern in self.exclude_regex):
                    logger.debug(f"Skipping excluded path (regex): {path}")
                    continue

                if path.is_file():
                    # Check if file matches include patterns (glob or regex)
                    glob_match = any(path.match(pattern) for pattern in self.include_patterns)
                    regex_match = any(pattern.search(path_str) for pattern in self.include_regex)
                    
                    if glob_match or regex_match:
                        yield path
                elif path.is_dir():
                    # Recursively crawl subdirectories
                    yield from self._crawl_directory(path, depth + 1)
                elif path.is_symlink() and self.follow_symlinks:
                    # Follow symlinks if enabled
                    target = path.resolve()
                    target_str = str(target)
                    
                    if target.is_dir() and target not in self.visited_dirs:
                        yield from self._crawl_directory(target, depth + 1)
                    elif target.is_file():
                        glob_match = any(target.match(pattern) for pattern in self.include_patterns)
                        regex_match = any(pattern.search(target_str) for pattern in self.include_regex)
                        
                        if glob_match or regex_match:
                            yield target
        except PermissionError:
            logger.warning(f"Permission denied: {directory}")
        except Exception as e:
            logger.error(f"Error crawling {directory}: {e}")


def crawl_directory(
    directory: Path,
    exclude_patterns: Optional[List[str]] = None,
    include_patterns: Optional[List[str]] = None,
    exclude_regex: Optional[List[str]] = None,
    include_regex: Optional[List[str]] = None,
    max_depth: Optional[int] = None,
    follow_symlinks: bool = False,
    polling_rate: Optional[float] = None,
    ignore_dot_dirs: bool = True,
) -> List[Path]:
    """
    Crawl a directory and return discovered files.

    Args:
        directory: The directory to crawl.
        exclude_patterns: Glob patterns to exclude from crawling (e.g., "*.tmp").
        include_patterns: Glob patterns to include in crawling (e.g., "*.txt").
        exclude_regex: Regex patterns to exclude from crawling.
        include_regex: Regex patterns to include in crawling (e.g., r".*\.csv$" for CSV files).
        max_depth: Maximum depth to crawl.
        follow_symlinks: Whether to follow symbolic links.
        polling_rate: Time in seconds to wait between file operations.
        ignore_dot_dirs: Whether to ignore directories starting with a dot (default: True).

    Returns:
        List of discovered file paths.
    """
    crawler = Crawler(
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
    return list(crawler.crawl())