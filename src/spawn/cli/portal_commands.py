"""
Commands for local portal operations.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

import click

from spawn.config import config
from spawn.github import (
    create_template_portal,
    configure_static_json,
    GitHubClient,
)

from spawn.cli.common import cli, logger


@cli.group()
def portal():
    """
    Local portal operations.
    """
    pass


@portal.command(name="create")
@click.option(
    "--name",
    required=True,
    help="Name for the forked repository",
)
@click.option(
    "--search-index",
    required=True,
    help="UUID of the Globus Search index",
)
@click.option(
    "--description",
    help="Description for the new repository",
)
@click.option(
    "--organization",
    help="Organization to create the fork in",
)
@click.option(
    "--token",
    help="GitHub personal access token (overrides config and environment)",
)
@click.option(
    "--username",
    help="GitHub username (overrides config and environment)",
)
@click.option(
    "--title",
    help="Title for the portal",
)
@click.option(
    "--subtitle",
    help="Subtitle for the portal",
)
@click.option(
    "--config-file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to additional configuration JSON file",
)
@click.option(
    "--enable-pages/--no-enable-pages",
    default=False,
    help="Whether to enable GitHub Pages for the repository",
)
@click.option(
    "--enable-actions/--no-enable-actions",
    default=False,
    help="Whether to enable GitHub Actions for the repository",
)
@click.option(
    "--pages-branch",
    default="main",
    help="Branch to publish GitHub Pages from (if --enable-pages is used)",
)
@click.option(
    "--pages-path",
    default="/",
    help="Directory to publish GitHub Pages from (if --enable-pages is used). Use '/' for root",
)
@click.option(
    "--clone-dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory to clone the repository into",
)
def create_portal_cmd(
    name: str,
    search_index: str,
    description: Optional[str],
    organization: Optional[str],
    token: Optional[str],
    username: Optional[str],
    title: Optional[str],
    subtitle: Optional[str],
    config_file: Optional[Path],
    enable_pages: bool,
    enable_actions: bool,
    pages_branch: str,
    pages_path: str,
    clone_dir: Optional[Path],
):
    """
    Create a Globus search portal locally.

    This command forks the Globus template search portal, configures it with the specified
    Globus Search index, and optionally enables GitHub Pages and GitHub Actions.
    All operations are performed locally.
    """
    # Get GitHub credentials from options or config
    github_token = token or config.github_token
    github_username = username or config.github_username

    # Load additional configuration if provided
    additional_config = None
    if config_file:
        try:
            with open(config_file, "r") as f:
                additional_config = json.load(f)
        except Exception as e:
            logger.error(f"Error loading configuration file: {e}")
            sys.exit(1)

    try:
        logger.info(f"Creating portal {name} locally")

        # Use a temporary directory if clone_dir is not provided
        if clone_dir is None:
            import tempfile

            clone_dir = Path(tempfile.mkdtemp())
            logger.info(f"Using temporary directory: {clone_dir}")

        # Step 1: Fork and clone the template portal
        fork_result = create_template_portal(
            new_name=name,
            description=description,
            organization=organization,
            token=github_token,
            username=github_username,
            clone_dir=clone_dir,
        )

        # Get repository owner
        owner = organization or github_username
        if not owner:
            # Try to get username from the fork result
            owner = fork_result["repository"].get("owner", {}).get("login")
            if not owner:
                raise ValueError("Could not determine repository owner")

        # Step 2: Configure the portal
        static_json_path = configure_static_json(
            repo_dir=clone_dir,
            search_index=search_index,
            portal_title=title,
            portal_subtitle=subtitle,
            additional_config=additional_config,
            push_to_github=True,
            repo_owner=owner,
            repo_name=name,
            token=github_token,
            username=github_username,
            commit_message="Configure portal",
            branch="main",
        )

        # Step 3: Enable GitHub Pages and Actions if requested
        if enable_pages or enable_actions:
            client = GitHubClient(token=github_token, username=github_username)

            if enable_pages:
                pages_result = client.enable_github_pages(
                    repo_owner=owner,
                    repo_name=name,
                    branch=pages_branch,
                    path=pages_path,
                )
                logger.info(f"Enabled GitHub Pages for {owner}/{name}")

            if enable_actions:
                actions_result = client.enable_github_actions(
                    repo_owner=owner,
                    repo_name=name,
                )
                logger.info(f"Enabled GitHub Actions for {owner}/{name}")

        # Return information about the created portal
        result = {
            "repository": fork_result["repository"],
            "portal_url": f"https://{owner}.github.io/{name}" if enable_pages else None,
            "repository_url": f"https://github.com/{owner}/{name}",
            "search_index": search_index,
            "clone_path": str(clone_dir),
        }

        logger.info(f"Portal creation completed")
        print(f"Repository URL: {result['repository_url']}")
        if enable_pages:
            print(f"Portal URL: {result['portal_url']}")
        print(f"Clone path: {result['clone_path']}")
        print(json.dumps(result, indent=2, default=str))

    except Exception as e:
        logger.error(f"Error creating portal: {e}")
        sys.exit(1)
