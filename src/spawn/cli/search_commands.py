"""
Commands for Globus Search operations.
"""

import logging
import sys
from typing import List, Optional

import click
import globus_sdk
from globus_sdk import SearchClient

from spawn.cli.common import cli, logger


@cli.group()
def search():
    """
    Globus Search operations.
    """
    pass


@search.command(name="create-index")
@click.option(
    "--display-name",
    required=True,
    help="Display name for the search index",
)
@click.option(
    "--description",
    help="Description for the search index",
)
@click.option(
    "--visible-to",
    multiple=True,
    help="Globus Auth identities that can see this index (can be used multiple times)",
)
def create_search_index(
    display_name: str,
    description: Optional[str],
    visible_to: List[str],
):
    """
    Create a new Globus Search index.

    Creates a new search index in Globus Search that can be used for indexing metadata.
    Requires a Globus Auth token with the 'search.create_index' scope.
    """
    try:
        # Get a Globus Auth token for Search
        app = globus_sdk.UserApp(
            "SPAwn CLI App", client_id="367628a1-4b6a-4176-82bd-422f071d1adc"
        )
        app.add_scope_requirements(
            {"search": [globus_sdk.scopes.SearchScopes.make_mutable("all")]}
        )
        search_client = SearchClient(app=app)

        # Set default visible_to if not provided
        visible_to_list = list(visible_to) if visible_to else ["public"]

        # Create the index
        logger.info(f"Creating Globus Search index: {display_name}")

        create_result = search_client.create_index(
            display_name=display_name,
            description=description or "",
        )

        # Print the result
        index_id = create_result["id"]
        logger.info(f"Successfully created search index: {index_id}")

        print(f"Search Index ID: {index_id}")
        print(f"Display Name: {display_name}")
        print(f"Description: {description or '(No description)'}")
        print(f"Visible To: {', '.join(visible_to_list)}")
        print(
            "\nYou can use this index ID in your configuration or with the --search-index option."
        )

    except Exception as e:
        logger.error(f"Error creating search index: {e}")
        sys.exit(1)
