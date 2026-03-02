"""
Configuration management for miswag-dbt-yml
"""
from pathlib import Path
from typing import Dict, Optional

from ruamel.yaml import YAML
from rich.console import Console

console = Console()


class Config:
    """Configuration manager for dbt-yml-gen"""

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize configuration

        Args:
            config_path: Path to config file (defaults.yml)
                        If None, looks for defaults.yml in current directory
        """
        self.yaml = YAML()
        self.config_path = config_path
        self.config_data: Dict = {}

        # Try to load config
        if config_path and config_path.exists():
            self._load_config(config_path)
        else:
            # Try to find defaults.yml in current directory
            default_path = Path.cwd() / "defaults.yml"
            if default_path.exists():
                self._load_config(default_path)
            else:
                console.print(
                    "[yellow]⚠[/yellow] No configuration file found, using built-in defaults"
                )

    def _load_config(self, config_path: Path):
        """
        Load configuration from YAML file

        Args:
            config_path: Path to config file
        """
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                self.config_data = self.yaml.load(f) or {}

            self.config_path = config_path
            console.print(f"[green]✓[/green] Loaded config from {config_path}")

        except Exception as e:
            console.print(
                f"[red]Error:[/red] Failed to load config from {config_path}: {str(e)}"
            )
            self.config_data = {}

    def get_clickhouse_config(self) -> Dict:
        """
        Get ClickHouse connection configuration

        Returns:
            Dict with connection parameters
        """
        ch_config = self.config_data.get("clickhouse", {})

        return {
            "host": ch_config.get("host", "localhost"),
            "port": ch_config.get("port", 8123),
            "username": ch_config.get("username", "default"),
            "password": ch_config.get("password", ""),
            "database": ch_config.get("database", "default"),
        }

    def get_defaults(self) -> Dict:
        """
        Get default values for metadata

        Returns:
            Dict with default values
        """
        return self.config_data.get("defaults", {})

    def get_domain_defaults(self, domain: str) -> Dict:
        """
        Get domain-specific default values

        Args:
            domain: Domain name (e.g., "operations", "search")

        Returns:
            Dict with domain defaults merged with global defaults
        """
        # Start with global defaults
        defaults = self.get_defaults().copy()

        # Merge domain-specific defaults
        domains_config = self.config_data.get("domains", {})
        if domain in domains_config:
            domain_defaults = domains_config[domain]
            defaults.update(domain_defaults)

        return defaults

    def get(self, key: str, default=None):
        """
        Get a configuration value by key

        Args:
            key: Configuration key (supports dot notation: "clickhouse.host")
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        keys = key.split(".")
        value = self.config_data

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def has_clickhouse_config(self) -> bool:
        """
        Check if ClickHouse configuration is present

        Returns:
            True if ClickHouse config exists, False otherwise
        """
        return "clickhouse" in self.config_data

    def save_config(self, output_path: Optional[Path] = None):
        """
        Save current configuration to file

        Args:
            output_path: Path to save config (uses self.config_path if None)
        """
        path = output_path or self.config_path

        if not path:
            console.print("[red]Error:[/red] No config path specified")
            return

        try:
            with open(path, "w", encoding="utf-8") as f:
                self.yaml.dump(self.config_data, f)

            console.print(f"[green]✓[/green] Saved config to {path}")

        except Exception as e:
            console.print(f"[red]Error:[/red] Failed to save config: {str(e)}")

    @staticmethod
    def create_default_config(output_path: Path):
        """
        Create a default configuration file template

        Args:
            output_path: Path where to create the config file
        """
        default_config = {
            "clickhouse": {
                "host": "localhost",
                "port": 8123,
                "username": "default",
                "password": "",
                "database": "analytics_db",
            },
            "defaults": {
                "tech_owner": "hameed_mahmood",
                "business_owner": "data_team",
                "data_classification": "internal",
                "source_system": "miswagdb",
                "data_loading": "mageai",
                "source_frequency": "24 hours",
                "update_frequency": "on_demand",
            },
            "domains": {
                "operations": {
                    "business_owner": "suhib_falih",
                    "source_system": "miswagdb",
                    "data_loading": "mageai",
                },
                "search": {
                    "business_owner": "data_team",
                    "source_system": "rudderstack",
                    "data_loading": "rudderstack",
                },
                "customer": {
                    "business_owner": "customer_success",
                    "source_system": "miswagdb",
                    "data_loading": "mageai",
                },
            },
        }

        try:
            yaml = YAML()
            yaml.indent(mapping=2, sequence=2, offset=2)

            with open(output_path, "w", encoding="utf-8") as f:
                yaml.dump(default_config, f)

            console.print(f"[green]✓[/green] Created default config at {output_path}")

        except Exception as e:
            console.print(f"[red]Error:[/red] Failed to create config: {str(e)}")

    def find_dbt_project_root(self) -> Optional[Path]:
        """
        Find dbt project root by looking for dbt_project.yml

        Searches current directory and parent directories

        Returns:
            Path to dbt project root, or None if not found
        """
        current = Path.cwd()

        # Check current directory and up to 5 levels up
        for _ in range(5):
            dbt_project_file = current / "dbt_project.yml"
            if dbt_project_file.exists():
                return current

            parent = current.parent
            if parent == current:  # Reached filesystem root
                break
            current = parent

        return None

    def get_dbt_project_name(self) -> Optional[str]:
        """
        Get dbt project name from dbt_project.yml

        Returns:
            Project name or None
        """
        project_root = self.find_dbt_project_root()

        if not project_root:
            return None

        try:
            dbt_project_file = project_root / "dbt_project.yml"
            with open(dbt_project_file, "r", encoding="utf-8") as f:
                dbt_config = self.yaml.load(f)

            return dbt_config.get("name")

        except Exception:
            return None
