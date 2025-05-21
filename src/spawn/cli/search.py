"""
Globus Search commands for SPAwn CLI.

Handles retrieval of entries from Globus Search.
"""

import logging
import sys
from typing import Optional

import click

from spawn.config import config
from spawn.globus.globus_search import GlobusSearchClient

logger = logging.getLogger(__name__)


@click.command()
@click.argument("subject", required=False)
@click.option("--search-index", help="Globus Search index UUID")
@click.option("--auth-token", help="Globus Auth token with search.ingest scope")
def get_entry(
    subject: Optional[str],
    search_index: Optional[str],
    auth_token: Optional[str] = None,
) -> None:
    """
    Get an entry from Globus Search.
    """
    index_uuid = search_index or config.globus_search_index
    if not index_uuid:
        logger.error("No Globus Search index UUID provided")
        sys.exit(1)
    token = auth_token or getattr(config, "globus_auth_token", None)
    client = GlobusSearchClient(
        index_uuid=index_uuid,
        auth_token=token,
    )
    if subject:
        entry = client.get_entry(subject)
        if entry:
            import json

            print(json.dumps(entry, indent=2, default=str))
        else:
            print(f"No entry found with subject: {subject}")
    else:
        print(f"Globus Search Index: {index_uuid}")
        print("Use --subject to get a specific entry")
