"""
Commands for GitHub repository operations.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Optional, List

import click

from spawn.config import config
from spawn.github import create_template_portal, configure_static_json, GitHubClient

from spawn.cli.common import cli, logger


@cli.group()
def github():
    """
    GitHub repository operations.
    """
    pass


@github.command(name="fork-portal")
@click.option(
    "--name",
    required=True,
    help="Name for the forked repository",
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
    "--clone-dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory to clone the repository into",
)
def fork_portal(
    name: str,
    description: Optional[str],
    organization: Optional[str],
    token: Optional[str],
    username: Optional[str],
    clone_dir: Optional[Path],
):
    """
    Fork the Globus template search portal.

    Creates a new GitHub repository by forking the Globus template search portal.
    Requires a GitHub personal access token with 'repo' scope.
    """
    try:
        result = create_template_portal(
            new_name=name,
            description=description,
            organization=organization,
            token=token,
            username=username,
            clone_dir=clone_dir,
        )

        print(f"Successfully forked repository: {result['repository']['html_url']}")

        if result["clone_path"]:
            print(f"Cloned repository to: {result['clone_path']}")
    except Exception as e:
        logger.error(f"Error forking repository: {e}")
        sys.exit(1)


@github.command(name="configure-portal")
@click.argument(
    "repo_dir", type=click.Path(exists=True, file_okay=False, path_type=Path)
)
@click.option(
    "--search-index",
    required=True,
    help="UUID of the Globus Search index",
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
    "--push/--no-push",
    default=False,
    help="Whether to push the changes to GitHub",
)
@click.option(
    "--repo-owner",
    help="Owner of the repository (required if --push is used)",
)
@click.option(
    "--repo-name",
    help="Name of the repository (required if --push is used)",
)
@click.option(
    "--token",
    help="GitHub personal access token (overrides config and environment)",
)
@click.option(
    "--commit-message",
    default="Configure portal",
    help="Commit message for the push",
)
@click.option(
    "--branch",
    default="main",
    help="Branch to push to",
)
@click.option(
    "--enable-pages/--no-enable-pages",
    default=False,
    help="Whether to enable GitHub Pages for the repository",
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
    "--enable-actions/--no-enable-actions",
    default=False,
    help="Whether to enable GitHub Actions for the repository",
)
def configure_portal(
    repo_dir: Path,
    search_index: str,
    title: Optional[str],
    subtitle: Optional[str],
    config_file: Optional[Path],
    push: bool,
    repo_owner: Optional[str],
    repo_name: Optional[str],
    token: Optional[str],
    commit_message: str,
    branch: str,
    enable_pages: bool,
    pages_branch: str,
    pages_path: str,
    enable_actions: bool,
):
    """
    Configure the static.json file in a Globus template search portal repository.

    REPO_DIR is the path to the repository directory.

    This command can also enable GitHub Pages and GitHub Actions for the repository
    to allow automatic publishing of the portal.
    """
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
        # Validate parameters
        if (push or enable_pages or enable_actions) and (
            not repo_owner or not repo_name
        ):
            logger.error(
                "--repo-owner and --repo-name are required when --push, --enable-pages, or --enable-actions is used"
            )
            sys.exit(1)

        # Configure static.json
        static_json_path = configure_static_json(
            repo_dir=repo_dir,
            search_index=search_index,
            portal_title=title,
            portal_subtitle=subtitle,
            additional_config=additional_config,
            push_to_github=push,
            repo_owner=repo_owner,
            repo_name=repo_name,
            token=token,
            commit_message=commit_message,
            branch=branch,
        )

        if push:
            print(
                f"Successfully configured static.json at: {static_json_path} and pushed to {repo_owner}/{repo_name}"
            )
        else:
            print(f"Successfully configured static.json at: {static_json_path}")

        # Enable GitHub Pages if requested
        if enable_pages:
            client = GitHubClient(token=token)
            pages_result = client.enable_github_pages(
                repo_owner=repo_owner,
                repo_name=repo_name,
                branch=pages_branch,
                path=pages_path,
            )
            print(f"Successfully enabled GitHub Pages for {repo_owner}/{repo_name}")
            if "html_url" in pages_result:
                print(f"Site URL: {pages_result['html_url']}")

        # Enable GitHub Actions if requested
        if enable_actions:
            client = GitHubClient(token=token)
            client.enable_github_actions(
                repo_owner=repo_owner,
                repo_name=repo_name,
            )
            print(f"Successfully enabled GitHub Actions for {repo_owner}/{repo_name}")
            print(
                f"GitHub Actions workflows can now automatically publish to GitHub Pages"
            )

    except Exception as e:
        logger.error(f"Error configuring portal: {e}")
        sys.exit(1)