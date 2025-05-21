"""
Command-line interface for SPAwn.

This module provides a command-line interface for the SPAwn tool.
This is a wrapper module that imports from the cli package.
"""

from spawn.cli import cli, main

# Re-export the CLI group and main function
__all__ = ["cli", "main"]

if __name__ == "__main__":
    main()
