"""
Main entry point for the SPAwn CLI.

This module imports all command groups and commands from the other CLI modules
and provides the main entry point for the CLI.
"""

# Import the main CLI group from common
from spawn.cli.common import cli

# Import all command modules to register their commands with the CLI group
import spawn.cli.crawl_commands
import spawn.cli.github_commands
import spawn.cli.compute_commands
import spawn.cli.flow_commands
import spawn.cli.search_commands


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
