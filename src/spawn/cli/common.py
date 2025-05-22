"""
Common utilities and shared code for the SPAwn CLI.
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import click

from spawn.config import config, load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "--config-file",
    "-c",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to configuration file",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.version_option()
def cli(config_file: Optional[Path], verbose: bool):
    """
    SPAwn - Static Portal Automatic web indexer.

    A tool for crawling directories, extracting metadata, and generating
    Globus Static Portals from Elasticsearch indices.
    """
    # Configure logging level
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose output enabled")

    # Load configuration
    if config_file:
        try:
            load_config(config_file)
            logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            sys.exit(1)
