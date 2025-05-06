"""
Configuration handling for SPAwn.

This module provides functionality for loading, validating, and accessing
configuration settings for the SPAwn tool.
"""

import os
import yaml
from pathlib import Path
from typing import Any, Dict, Optional, Union, List


class Config:
    """Configuration manager for SPAwn."""

    DEFAULT_CONFIG_PATHS = [
        Path("./spawn.yaml"),
        Path("~/.config/spawn/config.yaml").expanduser(),
        Path("/etc/spawn/config.yaml"),
    ]

    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """
        Initialize the configuration manager.

        Args:
            config_path: Path to the configuration file. If None, default paths will be checked.
        """
        self.config_data: Dict[str, Any] = {}
        self.config_path = Path(config_path) if config_path else None
        
        # Load configuration
        if self.config_path:
            self._load_config(self.config_path)
        else:
            self._load_default_config()

    def _load_config(self, config_path: Path) -> None:
        """
        Load configuration from a file.

        Args:
            config_path: Path to the configuration file.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
            yaml.YAMLError: If the configuration file is not valid YAML.
        """
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r") as f:
            self.config_data = yaml.safe_load(f) or {}

    def _load_default_config(self) -> None:
        """
        Load configuration from default paths.

        Tries to load configuration from the default paths in order.
        If no configuration file is found, an empty configuration is used.
        """
        for path in self.DEFAULT_CONFIG_PATHS:
            if path.exists():
                self._load_config(path)
                self.config_path = path
                return

        # No configuration file found, use empty configuration
        self.config_data = {}

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.

        Args:
            key: The configuration key.
            default: The default value to return if the key is not found.

        Returns:
            The configuration value, or the default value if the key is not found.
        """
        return self.config_data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value.

        Args:
            key: The configuration key.
            value: The configuration value.
        """
        self.config_data[key] = value

    def save(self, path: Optional[Union[str, Path]] = None) -> None:
        """
        Save the configuration to a file.

        Args:
            path: Path to save the configuration to. If None, the current config_path is used.

        Raises:
            ValueError: If no path is provided and no config_path is set.
        """
        save_path = Path(path) if path else self.config_path
        if not save_path:
            raise ValueError("No path provided and no config_path set")

        # Create directory if it doesn't exist
        save_path.parent.mkdir(parents=True, exist_ok=True)

        with open(save_path, "w") as f:
            yaml.dump(self.config_data, f, default_flow_style=False)

    @property
    def elasticsearch_host(self) -> str:
        """Get the Elasticsearch host."""
        return self.get("elasticsearch", {}).get("host", "localhost:9200")

    @property
    def elasticsearch_index(self) -> str:
        """Get the Elasticsearch index name."""
        return self.get("elasticsearch", {}).get("index", "spawn")

    @property
    def output_dir(self) -> Path:
        """Get the output directory."""
        output_dir = self.get("output", {}).get("dir", "./output")
        return Path(output_dir).expanduser().absolute()

    @property
    def crawler_plugins(self) -> Dict[str, Dict[str, Any]]:
        """Get the crawler plugins configuration."""
        return self.get("crawler", {}).get("plugins", {})
    
    @property
    def crawler_polling_rate(self) -> float:
        """
        Get the crawler polling rate in seconds.
        
        This controls how quickly the crawler processes files to avoid
        putting too much load on the filesystem.
        """
        return self.get("crawler", {}).get("polling_rate", 0.0)
    
    @property
    def crawler_exclude_regex(self) -> List[str]:
        """Get the regex patterns to exclude from crawling."""
        return self.get("crawler", {}).get("exclude_regex", [])
    
    @property
    def crawler_include_regex(self) -> List[str]:
        """Get the regex patterns to include in crawling."""
        return self.get("crawler", {}).get("include_regex", [])
    
    @property
    def crawler_ignore_dot_dirs(self) -> bool:
        """Get whether to ignore directories starting with a dot."""
        return self.get("crawler", {}).get("ignore_dot_dirs", True)
    
    @property
    def metadata_json_dir(self) -> Optional[Path]:
        """Get the directory to save metadata JSON files in."""
        json_dir = self.get("metadata", {}).get("json_dir")
        return Path(json_dir).expanduser().absolute() if json_dir else None
    
    @property
    def save_metadata_json(self) -> bool:
        """Get whether to save metadata as JSON files."""
        return self.get("metadata", {}).get("save_json", False)
    
    @property
    def github_token(self) -> Optional[str]:
        """Get the GitHub personal access token."""
        return self.get("github", {}).get("token") or os.environ.get("GITHUB_TOKEN")
    
    @property
    def github_username(self) -> Optional[str]:
        """Get the GitHub username."""
        return self.get("github", {}).get("username") or os.environ.get("GITHUB_USERNAME")
    
    @property
    def globus_search_index(self) -> Optional[str]:
        """Get the Globus Search index UUID."""
        return self.get("globus", {}).get("search_index") or os.environ.get("GLOBUS_SEARCH_INDEX")
    
    @property
    def globus_search_visible_to(self) -> List[str]:
        """Get the list of Globus Auth identities that can see entries."""
        return self.get("globus", {}).get("visible_to", ["public"])
    
    @property
    def globus_compute_endpoint_id(self) -> Optional[str]:
        """Get the Globus Compute endpoint ID."""
        return self.get("globus", {}).get("compute_endpoint_id") or os.environ.get("GLOBUS_COMPUTE_ENDPOINT_ID")
    
    @property
    def globus_flow_id(self) -> Optional[str]:
        """Get the Globus Flow ID."""
        return self.get("globus", {}).get("flow_id") or os.environ.get("GLOBUS_FLOW_ID")
    
    @property
    def globus_client_id(self) -> Optional[str]:
        """Get the Globus Auth client ID."""
        return self.get("globus", {}).get("client_id") or os.environ.get("GLOBUS_CLIENT_ID")
    
    @property
    def globus_client_secret(self) -> Optional[str]:
        """Get the Globus Auth client secret."""
        return self.get("globus", {}).get("client_secret") or os.environ.get("GLOBUS_CLIENT_SECRET")

    @property
    def portal_config(self) -> Dict[str, Any]:
        """Get the portal configuration."""
        return self.get("portal", {})


# Global configuration instance
config = Config()


def load_config(config_path: Optional[Union[str, Path]] = None) -> Config:
    """
    Load configuration from a file.

    Args:
        config_path: Path to the configuration file. If None, default paths will be checked.

    Returns:
        The configuration instance.
    """
    global config
    config = Config(config_path)
    return config