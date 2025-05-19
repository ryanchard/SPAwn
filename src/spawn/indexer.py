"""
Elasticsearch indexer for SPAwn.

This module provides functionality for indexing metadata in Elasticsearch.
"""

import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm

from spawn.config import config
from spawn.metadata import extract_metadata, save_metadata_to_json

logger = logging.getLogger(__name__)


class ElasticsearchIndexer:
    """Elasticsearch indexer for metadata."""

    def __init__(
        self,
        host: Optional[str] = None,
        index_name: Optional[str] = None,
        batch_size: int = 100,
        save_json: Optional[bool] = None,
        json_dir: Optional[Path] = None,
    ):
        """
        Initialize the Elasticsearch indexer.

        Args:
            host: Elasticsearch host URL.
            index_name: Name of the Elasticsearch index.
            batch_size: Number of documents to index in a single batch.
            save_json: Whether to save metadata as JSON files.
            json_dir: Directory to save JSON files in.
        """
        self.host = host or config.elasticsearch_host
        self.index_name = index_name or config.elasticsearch_index
        self.batch_size = batch_size
        self.save_json = (
            save_json if save_json is not None else config.save_metadata_json
        )
        self.json_dir = json_dir or config.metadata_json_dir
        self.es = None

    def connect(self) -> bool:
        """
        Connect to Elasticsearch.

        Returns:
            True if connection is successful, False otherwise.
        """
        try:
            self.es = Elasticsearch(self.host)
            if not self.es.ping():
                logger.error(f"Failed to connect to Elasticsearch at {self.host}")
                return False
            logger.info(f"Connected to Elasticsearch at {self.host}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to Elasticsearch: {e}")
            return False

    def create_index(self, mappings: Optional[Dict[str, Any]] = None) -> bool:
        """
        Create the Elasticsearch index.

        Args:
            mappings: Elasticsearch mappings for the index.

        Returns:
            True if index creation is successful, False otherwise.
        """
        if not self.es:
            if not self.connect():
                return False

        try:
            # Check if index exists
            if self.es.indices.exists(index=self.index_name):
                logger.info(f"Index {self.index_name} already exists")
                return True

            # Create index with mappings
            index_settings = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                },
                "mappings": mappings
                or {
                    "properties": {
                        "path": {"type": "keyword"},
                        "filename": {"type": "keyword"},
                        "extension": {"type": "keyword"},
                        "size_bytes": {"type": "long"},
                        "created_at": {"type": "date"},
                        "modified_at": {"type": "date"},
                        "accessed_at": {"type": "date"},
                        "mime_type": {"type": "keyword"},
                        "encoding": {"type": "keyword"},
                        "content_preview": {"type": "text"},
                        "line_count": {"type": "integer"},
                        "word_count": {"type": "integer"},
                        "char_count": {"type": "integer"},
                        "language": {"type": "keyword"},
                        "keywords": {"type": "keyword"},
                    }
                },
            }

            self.es.indices.create(index=self.index_name, body=index_settings)
            logger.info(f"Created index {self.index_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating index {self.index_name}: {e}")
            return False

    def index_file(self, file_path: Path) -> bool:
        """
        Index a single file.

        Args:
            file_path: Path to the file.

        Returns:
            True if indexing is successful, False otherwise.
        """
        if not self.es:
            if not self.connect():
                return False

        try:
            # Extract metadata
            metadata = extract_metadata(file_path)

            # Save metadata to JSON if enabled
            if self.save_json:
                try:
                    json_path = save_metadata_to_json(
                        file_path, metadata, self.json_dir
                    )
                    logger.debug(f"Saved metadata to JSON: {json_path}")
                except Exception as e:
                    logger.error(f"Error saving metadata to JSON for {file_path}: {e}")

            # Add document ID
            doc_id = str(file_path)

            # Index document
            self.es.index(index=self.index_name, id=doc_id, document=metadata)
            logger.debug(f"Indexed file: {file_path}")
            return True
        except Exception as e:
            logger.error(f"Error indexing file {file_path}: {e}")
            return False

    def index_files(self, file_paths: List[Path]) -> Dict[str, int]:
        """
        Index multiple files in batches.

        Args:
            file_paths: List of file paths to index.

        Returns:
            Dictionary with counts of successful and failed indexing operations.
        """
        if not self.es:
            if not self.connect():
                return {"success": 0, "failed": len(file_paths)}

        # Ensure index exists
        if not self.create_index():
            return {"success": 0, "failed": len(file_paths)}

        # Prepare for batch indexing
        success_count = 0
        failed_count = 0

        # Process files in batches
        with tqdm(total=len(file_paths), desc="Indexing") as pbar:
            batch = []

            for file_path in file_paths:
                try:
                    # Extract metadata
                    metadata = extract_metadata(file_path)

                    # Save metadata to JSON if enabled
                    if self.save_json:
                        try:
                            json_path = save_metadata_to_json(
                                file_path, metadata, self.json_dir
                            )
                            logger.debug(f"Saved metadata to JSON: {json_path}")
                        except Exception as e:
                            logger.error(
                                f"Error saving metadata to JSON for {file_path}: {e}"
                            )

                    # Add to batch
                    batch.append(
                        {
                            "_index": self.index_name,
                            "_id": str(file_path),
                            "_source": metadata,
                        }
                    )

                    # Process batch if it reaches batch size
                    if len(batch) >= self.batch_size:
                        success, failed = self._process_batch(batch)
                        success_count += success
                        failed_count += failed
                        batch = []

                    pbar.update(1)
                except Exception as e:
                    logger.error(f"Error preparing file {file_path} for indexing: {e}")
                    failed_count += 1
                    pbar.update(1)

            # Process remaining batch
            if batch:
                success, failed = self._process_batch(batch)
                success_count += success
                failed_count += failed

        logger.info(
            f"Indexed {success_count} files, failed to index {failed_count} files"
        )
        return {"success": success_count, "failed": failed_count}

    def _process_batch(self, batch: List[Dict[str, Any]]) -> tuple[int, int]:
        """
        Process a batch of documents.

        Args:
            batch: List of documents to index.

        Returns:
            Tuple of (success_count, failed_count).
        """
        try:
            # Use helpers.bulk for efficient batch indexing
            success, failed = helpers.bulk(
                self.es, batch, stats_only=True, raise_on_error=False
            )
            return success, len(batch) - success
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            return 0, len(batch)

    def refresh_index(self) -> None:
        """Refresh the index to make all operations available for search."""
        if not self.es:
            if not self.connect():
                return

        try:
            self.es.indices.refresh(index=self.index_name)
            logger.debug(f"Refreshed index {self.index_name}")
        except Exception as e:
            logger.error(f"Error refreshing index {self.index_name}: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the index.

        Returns:
            Dictionary of index statistics.
        """
        if not self.es:
            if not self.connect():
                return {"error": "Not connected to Elasticsearch"}

        try:
            # Get index stats
            stats = self.es.indices.stats(index=self.index_name)

            # Get count
            count = self.es.count(index=self.index_name)

            return {
                "doc_count": count["count"],
                "index_size_bytes": stats["indices"][self.index_name]["total"]["store"][
                    "size_in_bytes"
                ],
                "index_name": self.index_name,
            }
        except Exception as e:
            logger.error(f"Error getting index stats: {e}")
            return {"error": str(e)}


def index_files(
    file_paths: List[Path],
    host: Optional[str] = None,
    index_name: Optional[str] = None,
    batch_size: int = 100,
    save_json: Optional[bool] = None,
    json_dir: Optional[Path] = None,
) -> Dict[str, int]:
    """
    Index files in Elasticsearch.

    Args:
        file_paths: List of file paths to index.
        host: Elasticsearch host URL.
        index_name: Name of the Elasticsearch index.
        batch_size: Number of documents to index in a single batch.
        save_json: Whether to save metadata as JSON files.
        json_dir: Directory to save JSON files in.

    Returns:
        Dictionary with counts of successful and failed indexing operations.
    """
    indexer = ElasticsearchIndexer(
        host=host,
        index_name=index_name,
        batch_size=batch_size,
        save_json=save_json,
        json_dir=json_dir,
    )
    return indexer.index_files(file_paths)
