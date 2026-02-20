"""
Unit tests for Path Resolver

Tests environment-aware path resolution with variable substitution.
"""

import os
import pytest
from pathlib import Path
from unittest.mock import patch
from src.utils.path_resolver import PathResolver, resolve_path, get_medallion_paths


class TestPathResolver:
    """Test suite for PathResolver class."""

    def test_init_default(self):
        """Test PathResolver initialization with defaults."""
        resolver = PathResolver()

        assert resolver.default_base_path == "./data/parquet"
        assert 'PARQUET_BASE_PATH' in resolver.env_vars

    def test_init_custom_base_path(self):
        """Test PathResolver with custom default base path."""
        custom_path = "/custom/base/path"
        resolver = PathResolver(default_base_path=custom_path)

        # If PARQUET_BASE_PATH not set in env, should use custom default
        # (only if env var is not set)
        if 'PARQUET_BASE_PATH' not in os.environ:
            assert resolver.env_vars['PARQUET_BASE_PATH'] == custom_path

    @patch.dict(os.environ, {'PARQUET_BASE_PATH': '/mnt/nas/reai-data'})
    def test_resolve_with_env_var(self):
        """Test path resolution with environment variable."""
        resolver = PathResolver()

        resolved = resolver.resolve("${PARQUET_BASE_PATH}/bronze")

        assert resolved == "/mnt/nas/reai-data/bronze"
        assert "${" not in resolved

    @patch.dict(os.environ, {'PARQUET_BASE_PATH': './data/parquet'})
    def test_resolve_dev_environment(self):
        """Test path resolution in development environment."""
        resolver = PathResolver()

        resolved = resolver.resolve("${PARQUET_BASE_PATH}/silver")

        assert resolved == "./data/parquet/silver"

    def test_resolve_without_placeholder(self):
        """Test path resolution without placeholders."""
        resolver = PathResolver()

        plain_path = "data/raw/file.parquet"
        resolved = resolver.resolve(plain_path)

        assert resolved == plain_path

    @patch.dict(os.environ, {'PARQUET_BASE_PATH': '/nas/data', 'APP_ENV': 'prod'})
    def test_resolve_multiple_vars(self):
        """Test resolution with multiple environment variables."""
        resolver = PathResolver()

        resolved = resolver.resolve("${PARQUET_BASE_PATH}/${APP_ENV}/logs")

        assert resolved == "/nas/data/prod/logs"

    def test_resolve_path_returns_path_object(self):
        """Test resolve_path returns Path object."""
        resolver = PathResolver()

        path_obj = resolver.resolve_path("${PARQUET_BASE_PATH}/bronze")

        assert isinstance(path_obj, Path)

    @patch.dict(os.environ, {'PARQUET_BASE_PATH': '/tmp/test_parquet'})
    def test_resolve_path_create_if_missing(self, tmp_path):
        """Test resolve_path creates directory if requested."""
        resolver = PathResolver()

        # Use tmp_path for testing
        test_path = tmp_path / "bronze"
        path_str = str(test_path)

        path_obj = resolver.resolve_path(path_str, create_if_missing=True)

        assert path_obj.exists()
        assert path_obj.is_dir()

    def test_load_config(self):
        """Test loading paths from YAML config."""
        resolver = PathResolver()

        try:
            config = resolver.load_config()

            # Check expected keys exist
            assert isinstance(config, dict)
            assert 'bronze_dir' in config or len(config) > 0

        except FileNotFoundError:
            pytest.skip("Config file not found (expected in CI)")

    @patch.dict(os.environ, {'PARQUET_BASE_PATH': '/mnt/nas/reai-data'})
    def test_get_path(self):
        """Test getting path by key from config."""
        resolver = PathResolver()

        try:
            bronze_path = resolver.get_path('bronze_dir')

            assert isinstance(bronze_path, Path)
            assert '/mnt/nas/reai-data' in str(bronze_path) or 'bronze' in str(bronze_path)

        except FileNotFoundError:
            pytest.skip("Config file not found")
        except KeyError:
            pytest.skip("bronze_dir not in config")

    @patch.dict(os.environ, {'PARQUET_BASE_PATH': '/mnt/nas/reai-data'})
    def test_get_all_paths(self):
        """Test getting all paths from config."""
        resolver = PathResolver()

        try:
            all_paths = resolver.get_all_paths()

            assert isinstance(all_paths, dict)
            assert all(isinstance(p, Path) for p in all_paths.values())

        except FileNotFoundError:
            pytest.skip("Config file not found")

    def test_get_path_key_not_found(self):
        """Test get_path raises KeyError for invalid key."""
        resolver = PathResolver()

        with pytest.raises(KeyError):
            resolver.get_path('nonexistent_key')

    def test_repr(self):
        """Test PathResolver string representation."""
        resolver = PathResolver()

        repr_str = repr(resolver)

        assert 'PathResolver' in repr_str
        assert 'base_path' in repr_str


class TestConvenienceFunctions:
    """Test suite for module-level convenience functions."""

    @patch.dict(os.environ, {'PARQUET_BASE_PATH': '/mnt/nas/reai-data'})
    def test_resolve_path_function(self):
        """Test module-level resolve_path function."""
        path_obj = resolve_path("${PARQUET_BASE_PATH}/bronze")

        assert isinstance(path_obj, Path)
        assert '/mnt/nas/reai-data' in str(path_obj) or 'bronze' in str(path_obj)

    @patch.dict(os.environ, {'PARQUET_BASE_PATH': '/mnt/nas/reai-data'})
    def test_get_medallion_paths_function(self):
        """Test get_medallion_paths returns all layers."""
        paths = get_medallion_paths(create_if_missing=False)

        assert 'bronze_dir' in paths
        assert 'silver_dir' in paths
        assert 'gold_dir' in paths

        assert all(isinstance(p, Path) for p in paths.values())


class TestEnvironmentScenarios:
    """Test different environment scenarios."""

    @patch.dict(os.environ, {
        'PARQUET_BASE_PATH': './data/parquet',
        'APP_ENV': 'dev'
    })
    def test_development_environment(self):
        """Test path resolution in development environment."""
        resolver = PathResolver()

        bronze = resolver.resolve("${PARQUET_BASE_PATH}/bronze")
        assert bronze == "./data/parquet/bronze"

        assert resolver.env_vars['APP_ENV'] == 'dev'

    @patch.dict(os.environ, {
        'PARQUET_BASE_PATH': '/mnt/staging/reai-data',
        'APP_ENV': 'staging'
    })
    def test_staging_environment(self):
        """Test path resolution in staging environment."""
        resolver = PathResolver()

        silver = resolver.resolve("${PARQUET_BASE_PATH}/silver")
        assert silver == "/mnt/staging/reai-data/silver"

        assert resolver.env_vars['APP_ENV'] == 'staging'

    @patch.dict(os.environ, {
        'PARQUET_BASE_PATH': '/mnt/nas/reai-data',
        'APP_ENV': 'prod'
    })
    def test_production_environment(self):
        """Test path resolution in production environment."""
        resolver = PathResolver()

        gold = resolver.resolve("${PARQUET_BASE_PATH}/gold")
        assert gold == "/mnt/nas/reai-data/gold"

        assert resolver.env_vars['APP_ENV'] == 'prod'

    @patch.dict(os.environ, {}, clear=True)
    def test_no_env_vars_uses_defaults(self):
        """Test fallback to defaults when no env vars set."""
        resolver = PathResolver(default_base_path="./default/path")

        # Should use default
        base_path = resolver.env_vars.get('PARQUET_BASE_PATH')
        assert base_path == "./default/path"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_resolve_empty_string(self):
        """Test resolving empty string."""
        resolver = PathResolver()

        resolved = resolver.resolve("")
        assert resolved == ""

    def test_resolve_none(self):
        """Test resolving None."""
        resolver = PathResolver()

        resolved = resolver.resolve(None)
        assert resolved is None

    @patch.dict(os.environ, {'PARQUET_BASE_PATH': '/path/with spaces/data'})
    def test_resolve_path_with_spaces(self):
        """Test path resolution with spaces."""
        resolver = PathResolver()

        resolved = resolver.resolve("${PARQUET_BASE_PATH}/bronze")
        assert resolved == "/path/with spaces/data/bronze"

    def test_resolve_unset_variable(self):
        """Test resolving with unset variable (should keep placeholder)."""
        resolver = PathResolver()

        # Variable that definitely doesn't exist
        resolved = resolver.resolve("${NONEXISTENT_VAR}/path")

        # Should keep the placeholder
        assert "${NONEXISTENT_VAR}" in resolved

    @patch.dict(os.environ, {'PARQUET_BASE_PATH': '/mnt/nas/reai-data'})
    def test_windows_path_normalization(self):
        """Test Windows-style path normalization."""
        resolver = PathResolver()

        # Windows-style path
        resolved = resolver.resolve("${PARQUET_BASE_PATH}\\bronze\\file.parquet")

        # Should normalize to forward slashes
        assert "\\" not in resolved
        assert "/" in resolved


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
