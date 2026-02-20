"""
Path Resolver for Environment-Aware Path Configuration

This module provides functionality to resolve file paths with environment variable
substitution, enabling seamless switching between development and production environments.

Features:
- Placeholder substitution (${VARIABLE} syntax)
- Environment variable resolution
- Default fallback values
- YAML config file support

Usage:
    >>> from utils.path_resolver import PathResolver
    >>> resolver = PathResolver()
    >>> bronze_path = resolver.resolve("${PARQUET_BASE_PATH}/bronze")
    >>> # Returns: "/mnt/nas/reai-data/bronze" (production)
    >>> # Or: "./data/parquet/bronze" (development)
"""

import os
import re
from pathlib import Path
from typing import Dict, Any, Optional
import yaml


class PathResolver:
    """Resolves file paths with environment variable substitution.

    Attributes:
        config_path: Path to YAML config file (default: config/paths.yml)
        env_vars: Dictionary of environment variables for substitution
        default_base_path: Default PARQUET_BASE_PATH if not set in env
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        default_base_path: str = "./data/parquet"
    ):
        """Initialize PathResolver.

        Args:
            config_path: Path to paths.yml config file (optional)
            default_base_path: Default PARQUET_BASE_PATH if env var not set
        """
        self.config_path = config_path or self._get_default_config_path()
        self.default_base_path = default_base_path
        self.env_vars = self._load_env_vars()
        self._config_cache: Optional[Dict[str, Any]] = None

    def _get_default_config_path(self) -> str:
        """Get default config path relative to project root."""
        # Assume this file is in src/utils/, go up 2 levels to project root
        current_file = Path(__file__)
        project_root = current_file.parent.parent.parent
        return str(project_root / "config" / "paths.yml")

    def _load_env_vars(self) -> Dict[str, str]:
        """Load environment variables for path substitution.

        Returns:
            Dictionary of environment variables with defaults
        """
        env_vars = {}

        # PARQUET_BASE_PATH (critical for NAS-first architecture)
        env_vars['PARQUET_BASE_PATH'] = os.getenv(
            'PARQUET_BASE_PATH',
            self.default_base_path
        )

        # APP_ENV (dev/staging/prod)
        env_vars['APP_ENV'] = os.getenv('APP_ENV', 'dev')

        # Add any other environment variables needed
        for key, value in os.environ.items():
            if key.startswith('DATA_') or key.startswith('PATH_'):
                env_vars[key] = value

        return env_vars

    def resolve(self, path_str: str) -> str:
        """Resolve path string with environment variable substitution.

        Supports ${VARIABLE} syntax for placeholder substitution.

        Args:
            path_str: Path string with optional ${VARIABLE} placeholders

        Returns:
            Resolved path string with variables substituted

        Examples:
            >>> resolver.resolve("${PARQUET_BASE_PATH}/bronze")
            '/mnt/nas/reai-data/bronze'

            >>> resolver.resolve("data/raw")
            'data/raw'
        """
        if not path_str:
            return path_str

        # Pattern to match ${VARIABLE} or $VARIABLE
        pattern = re.compile(r'\$\{([^}]+)\}|\$([A-Z_][A-Z0-9_]*)')

        def replacer(match):
            # Get variable name from either ${VAR} or $VAR format
            var_name = match.group(1) or match.group(2)

            # Look up in env_vars dict
            return self.env_vars.get(var_name, f"${{{var_name}}}")

        resolved = pattern.sub(replacer, path_str)

        # Normalize path separators
        resolved = resolved.replace('\\', '/')

        return resolved

    def resolve_path(self, path_str: str, create_if_missing: bool = False) -> Path:
        """Resolve path string and return as Path object.

        Args:
            path_str: Path string with optional ${VARIABLE} placeholders
            create_if_missing: Create directory if it doesn't exist

        Returns:
            Resolved Path object
        """
        resolved_str = self.resolve(path_str)
        path_obj = Path(resolved_str)

        if create_if_missing and not path_obj.exists():
            path_obj.mkdir(parents=True, exist_ok=True)

        return path_obj

    def load_config(self, reload: bool = False) -> Dict[str, Any]:
        """Load paths from YAML config file.

        Args:
            reload: Force reload from file (ignore cache)

        Returns:
            Dictionary of path configurations

        Raises:
            FileNotFoundError: If config file doesn't exist
        """
        if self._config_cache is not None and not reload:
            return self._config_cache

        config_path = Path(self.config_path)

        if not config_path.exists():
            raise FileNotFoundError(
                f"Config file not found: {config_path}\n"
                f"Please create config/paths.yml with path definitions."
            )

        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}

        self._config_cache = config
        return config

    def get_path(self, key: str, create_if_missing: bool = False) -> Path:
        """Get resolved path from config by key.

        Args:
            key: Path key in config (e.g., 'bronze_dir', 'silver_dir')
            create_if_missing: Create directory if it doesn't exist

        Returns:
            Resolved Path object

        Raises:
            KeyError: If key not found in config

        Examples:
            >>> resolver.get_path('bronze_dir')
            Path('/mnt/nas/reai-data/bronze')
        """
        config = self.load_config()

        if key not in config:
            raise KeyError(
                f"Path key '{key}' not found in config.\n"
                f"Available keys: {list(config.keys())}"
            )

        path_str = config[key]
        return self.resolve_path(path_str, create_if_missing=create_if_missing)

    def get_all_paths(self, create_if_missing: bool = False) -> Dict[str, Path]:
        """Get all resolved paths from config.

        Args:
            create_if_missing: Create directories if they don't exist

        Returns:
            Dictionary mapping path keys to resolved Path objects
        """
        config = self.load_config()

        resolved_paths = {}
        for key, path_str in config.items():
            if isinstance(path_str, str):
                resolved_paths[key] = self.resolve_path(
                    path_str,
                    create_if_missing=create_if_missing
                )

        return resolved_paths

    def __repr__(self) -> str:
        """String representation of PathResolver."""
        return (
            f"PathResolver("
            f"config='{self.config_path}', "
            f"base_path='{self.env_vars.get('PARQUET_BASE_PATH')}'"
            f")"
        )


# Global singleton instance for convenience
_default_resolver: Optional[PathResolver] = None


def get_resolver(force_new: bool = False) -> PathResolver:
    """Get global PathResolver instance (singleton pattern).

    Args:
        force_new: Create new resolver instead of using cached instance

    Returns:
        PathResolver instance
    """
    global _default_resolver

    if _default_resolver is None or force_new:
        _default_resolver = PathResolver()

    return _default_resolver


def resolve_path(path_str: str, create_if_missing: bool = False) -> Path:
    """Convenience function to resolve path using global resolver.

    Args:
        path_str: Path string with optional ${VARIABLE} placeholders
        create_if_missing: Create directory if it doesn't exist

    Returns:
        Resolved Path object

    Examples:
        >>> from utils.path_resolver import resolve_path
        >>> bronze = resolve_path("${PARQUET_BASE_PATH}/bronze", create_if_missing=True)
    """
    resolver = get_resolver()
    return resolver.resolve_path(path_str, create_if_missing=create_if_missing)


def get_medallion_paths(create_if_missing: bool = True) -> Dict[str, Path]:
    """Get all medallion architecture paths (bronze, silver, gold).

    Args:
        create_if_missing: Create directories if they don't exist

    Returns:
        Dictionary with keys: bronze_dir, silver_dir, gold_dir

    Examples:
        >>> from utils.path_resolver import get_medallion_paths
        >>> paths = get_medallion_paths()
        >>> bronze = paths['bronze_dir']
    """
    resolver = get_resolver()

    medallion_keys = ['bronze_dir', 'silver_dir', 'gold_dir']
    paths = {}

    for key in medallion_keys:
        try:
            paths[key] = resolver.get_path(key, create_if_missing=create_if_missing)
        except KeyError:
            # Fallback if key not in config
            layer_name = key.replace('_dir', '')
            paths[key] = resolver.resolve_path(
                f"${{PARQUET_BASE_PATH}}/{layer_name}",
                create_if_missing=create_if_missing
            )

    return paths
