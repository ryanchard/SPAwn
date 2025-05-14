"""
GitHub commands for SPAwn CLI.

Handles GitHub repository operations for portals.
"""
from typing import Optional
from pathlib import Path
import click
import logging
import sys

from spawn.utils.github import fork_template_portal, configure_static_json, GitHubClient

logger = logging.getLogger(__name__)

@click.group()
def github() -> None:
    """GitHub repository operations."""
    pass

@github.command(name="fork-portal")
@click.option("--name", required=True, help="Name for the forked repository")
@click.option("--description", help="Description for the new repository")
@click.option("--organization", help="Organization to create the fork in")
@click.option("--token", help="GitHub personal access token (overrides config and environment)")
@click.option("--username", help="GitHub username (overrides config and environment)")
@click.option("--clone-dir", type=click.Path(file_okay=False, path_type=Path), help="Directory to clone the repository into")
@click.option("--pages-branch", default="gh-pages", help="Branch to deploy GitHub Pages from (default: gh-pages)") # TODO: Ask ryan about this
@click.option("--pages-path", default="/", help="Path in the repo to deploy for GitHub Pages (default: /)")
def fork_portal(
    name: str,
    description: Optional[str],
    organization: Optional[str],
    token: Optional[str],
    username: Optional[str],
    clone_dir: Optional[Path],
    pages_branch: str,
    pages_path: str,
) -> None:
    """
    Fork the Globus template search portal and configure GitHub Pages and Actions.
    """

    repo_owner = organization or username or result['repository'].get('owner', {}).get('login')
    repo_name = name
    client = GitHubClient(token=token, username=username)
    
    try:
        result = client.fork_template_portal(
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

        # Configure GitHub Pages and Actions
        try:
            client.configure_pages_and_actions(
                repo_owner=repo_owner,
                repo_name=repo_name,
                branch=pages_branch,
                pages_path=pages_path,
            )
            print(f"Configured GitHub Pages and Actions for {repo_owner}/{repo_name} (branch: {pages_branch}, path: {pages_path})")
        except Exception as e:
            logger.error(f"Error configuring GitHub Pages and Actions: {e}")
            print(f"Warning: Repository forked, but failed to configure Pages/Actions: {e}")
    except Exception as e:
        logger.error(f"Error forking repository: {e}")
        sys.exit(1)

@github.command(name="configure-portal")
@click.argument("repo_dir", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--index-name", required=True, help="UUID of the Globus Search index")
@click.option("--title", help="Title for the portal")
@click.option("--subtitle", help="Subtitle for the portal")
@click.option("--config-file", type=click.Path(exists=True, dir_okay=False, path_type=Path), help="Path to additional configuration JSON file")
@click.option("--push/--no-push", default=False, help="Whether to push the changes to GitHub")
@click.option("--repo-owner", help="Owner of the repository (required if --push is used)")
@click.option("--repo-name", help="Name of the repository (required if --push is used)")
@click.option("--token", help="GitHub personal access token (overrides config and environment)")
@click.option("--commit-message", default="Configure portal", help="Commit message for the push")
@click.option("--branch", default="main", help="Branch to push to")
def configure_portal(
    repo_dir: Path,
    index_name: str,
    title: Optional[str],
    subtitle: Optional[str],
    config_file: Optional[Path],
    push: bool,
    repo_owner: Optional[str],
    repo_name: Optional[str],
    token: Optional[str],
    commit_message: str,
    branch: str,
) -> None:
    """
    Configure the static.json file in a Globus template search portal repository.
    """
    import json
    additional_config = None
    if config_file:
        try:
            with open(config_file, "r") as f:
                additional_config = json.load(f)
        except Exception as e:
            logger.error(f"Error loading configuration file: {e}")
            sys.exit(1)
    try:
        if push and (not repo_owner or not repo_name):
            logger.error("--repo-owner and --repo-name are required when --push is used")
            sys.exit(1)
        static_json_path = configure_static_json(
            repo_dir=repo_dir,
            index_name=index_name,
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
            print(f"Successfully configured static.json at: {static_json_path} and pushed to {repo_owner}/{repo_name}")
        else:
            print(f"Successfully configured static.json at: {static_json_path}")
    except Exception as e:
        logger.error(f"Error configuring static.json: {e}")
        sys.exit(1) 