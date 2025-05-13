"""
Main entry point for the SPAwn CLI.

This module sets up the main CLI group, logging, and configuration, and registers all subcommands.
"""
from typing import Optional
from pathlib import Path
import logging
import sys
import click

from spawn.config import load_config

# Import subcommands/groups
from .crawl import crawl
from .extract import extract_file_metadata
from .github import github
from .compute import compute
from .flow import flow
from .search import get_entry

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
def cli(config_file: Optional[Path], verbose: bool) -> None:
    """
    SPAwn - Static Portal Automatic web indexer.

    A tool for crawling directories, extracting metadata, and generating
    Globus Static Portals from Elasticsearch indices.
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose output enabled")
    if config_file:
        try:
            load_config(config_file)
            logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            sys.exit(1)

# Register all subcommands/groups
cli.add_command(crawl)
cli.add_command(extract_file_metadata)
cli.add_command(github)
cli.add_command(compute)
cli.add_command(flow)
cli.add_command(get_entry)

def main() -> None:
    """Main entry point for the CLI."""
    cli()

if __name__ == "__main__":
    main() 