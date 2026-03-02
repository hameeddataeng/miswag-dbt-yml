"""
SQL parser for dbt model files
"""
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

import sqlparse
from rich.console import Console

console = Console()


@dataclass
class DBTModel:
    """Parsed dbt model information"""

    name: str
    file_path: Path
    materialization: Optional[str] = None
    columns: List[str] = field(default_factory=list)
    sources: List[Tuple[str, str]] = field(default_factory=list)  # (source, table)
    refs: List[str] = field(default_factory=list)
    config: Dict = field(default_factory=dict)
    has_jinja: bool = False


class SQLParser:
    """Parser for dbt SQL files"""

    def __init__(self):
        pass

    def parse_model_file(self, file_path: Path) -> Optional[DBTModel]:
        """
        Parse a dbt model SQL file

        Args:
            file_path: Path to the SQL file

        Returns:
            DBTModel object or None if parsing fails
        """
        if not file_path.exists():
            console.print(f"[red]Error:[/red] File not found: {file_path}")
            return None

        try:
            content = file_path.read_text(encoding="utf-8")

            # Extract model name from filename
            model_name = file_path.stem

            model = DBTModel(name=model_name, file_path=file_path)

            # Parse config block
            model.config = self._extract_config(content)
            model.materialization = model.config.get("materialized")

            # Parse sources and refs
            model.sources = self._extract_sources(content)
            model.refs = self._extract_refs(content)

            # Check for Jinja
            model.has_jinja = "{{" in content or "{%" in content

            # Extract columns from SELECT statement
            model.columns = self._extract_columns(content)

            console.print(
                f"[green]✓[/green] Parsed model: {model_name} ({len(model.columns)} columns)"
            )

            return model

        except Exception as e:
            console.print(f"[red]Error:[/red] Failed to parse {file_path}: {str(e)}")
            return None

    def _extract_config(self, content: str) -> Dict:
        """
        Extract config block from dbt model

        Example:
            {{ config(
                materialized='table',
                engine='MergeTree()',
                order_by='(id)'
            ) }}

        Returns:
            Dict of config parameters
        """
        config = {}

        # Match config block
        config_pattern = r"{{\s*config\s*\((.*?)\)\s*}}"
        match = re.search(config_pattern, content, re.DOTALL | re.IGNORECASE)

        if match:
            config_content = match.group(1)

            # Extract key-value pairs
            # Handle both single and double quotes
            param_pattern = r"(\w+)\s*=\s*['\"]([^'\"]+)['\"]"
            for param_match in re.finditer(param_pattern, config_content):
                key = param_match.group(1)
                value = param_match.group(2)
                config[key] = value

        return config

    def _extract_sources(self, content: str) -> List[Tuple[str, str]]:
        """
        Extract source() references from dbt model

        Example:
            {{ source('mysql_replication', 'mysql_op_stock') }}

        Returns:
            List of (source_name, table_name) tuples
        """
        sources = []

        # Match source references
        source_pattern = r"{{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*}}"

        for match in re.finditer(source_pattern, content):
            source_name = match.group(1)
            table_name = match.group(2)
            sources.append((source_name, table_name))

        return sources

    def _extract_refs(self, content: str) -> List[str]:
        """
        Extract ref() references from dbt model

        Example:
            {{ ref('stg_op_stock') }}

        Returns:
            List of referenced model names
        """
        refs = []

        # Match ref references
        ref_pattern = r"{{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*}}"

        for match in re.finditer(ref_pattern, content):
            ref_name = match.group(1)
            refs.append(ref_name)

        return refs

    def _extract_columns(self, content: str) -> List[str]:
        """
        Extract column names from SELECT statement

        Attempts to parse the SQL and extract column names/aliases

        Returns:
            List of column names
        """
        columns = []

        try:
            # Remove Jinja blocks temporarily for parsing
            cleaned_content = self._remove_jinja(content)

            # Parse SQL
            parsed = sqlparse.parse(cleaned_content)

            if not parsed:
                return columns

            # Find the SELECT statement
            for statement in parsed:
                if statement.get_type() == "SELECT":
                    columns = self._extract_select_columns(statement)
                    break

            # If no columns found, try simple regex extraction
            if not columns:
                columns = self._extract_columns_regex(content)

        except Exception as e:
            console.print(f"[yellow]⚠[/yellow] Column extraction failed: {str(e)}")
            # Fallback to regex
            columns = self._extract_columns_regex(content)

        return columns

    def _remove_jinja(self, content: str) -> str:
        """
        Remove Jinja blocks from SQL content for parsing

        Replaces source() and ref() with placeholder table names
        """
        # Replace source() with placeholder
        content = re.sub(
            r"{{\s*source\s*\([^)]+\)\s*}}", "placeholder_source", content
        )

        # Replace ref() with placeholder
        content = re.sub(r"{{\s*ref\s*\([^)]+\)\s*}}", "placeholder_ref", content)

        # Remove other Jinja blocks
        content = re.sub(r"{{.*?}}", "", content, flags=re.DOTALL)
        content = re.sub(r"{%.*?%}", "", content, flags=re.DOTALL)

        return content

    def _extract_select_columns(self, statement) -> List[str]:
        """
        Extract column names/aliases from parsed SELECT statement

        Args:
            statement: sqlparse Statement object

        Returns:
            List of column names
        """
        columns = []

        # Find the column list in SELECT clause
        in_select = False
        for token in statement.tokens:
            if token.ttype is sqlparse.tokens.Keyword.DML and token.value.upper() == "SELECT":
                in_select = True
                continue

            if in_select:
                if token.ttype is sqlparse.tokens.Keyword:
                    break

                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        col_name = self._get_column_name(identifier)
                        if col_name and col_name != "*":
                            columns.append(col_name)

                elif isinstance(token, sqlparse.sql.Identifier):
                    col_name = self._get_column_name(token)
                    if col_name and col_name != "*":
                        columns.append(col_name)

        return columns

    def _get_column_name(self, identifier) -> Optional[str]:
        """
        Get column name or alias from identifier

        Prefers alias if present, otherwise returns column name
        """
        # Check for alias
        if hasattr(identifier, "get_alias") and identifier.get_alias():
            return identifier.get_alias()

        # Get name
        if hasattr(identifier, "get_name") and identifier.get_name():
            return identifier.get_name()

        # Fallback to string representation
        name = str(identifier).strip()
        if name and name != "*":
            return name

        return None

    def _extract_columns_regex(self, content: str) -> List[str]:
        """
        Extract columns using regex as fallback

        Looks for patterns like:
            column_name,
            column_name AS alias,
            expression AS alias,

        Returns:
            List of column names
        """
        columns = []

        # Find SELECT clause
        select_match = re.search(
            r"\bSELECT\b(.*?)\bFROM\b", content, re.DOTALL | re.IGNORECASE
        )

        if not select_match:
            return columns

        select_clause = select_match.group(1)

        # Extract columns with aliases
        # Pattern: anything AS alias_name
        alias_pattern = r"(?:.*?)\s+AS\s+(\w+)"
        for match in re.finditer(alias_pattern, select_clause, re.IGNORECASE):
            alias = match.group(1)
            if alias.lower() not in ["select", "from", "where"]:
                columns.append(alias)

        # Extract simple column names (no AS)
        if not columns:
            # Split by comma and extract identifiers
            parts = select_clause.split(",")
            for part in parts:
                # Remove comments
                part = re.sub(r"--.*$", "", part, flags=re.MULTILINE)
                part = part.strip()

                if not part or part == "*":
                    continue

                # Extract last identifier (could be column name)
                identifier_match = re.search(r"\b(\w+)\b\s*$", part)
                if identifier_match:
                    col_name = identifier_match.group(1)
                    if col_name.lower() not in ["select", "from", "where", "and", "or"]:
                        columns.append(col_name)

        return columns

    def detect_domain_from_path(self, file_path: Path) -> Optional[str]:
        """
        Detect domain from file path

        Examples:
            models/operations/staging/... -> "operations"
            models/staging/operations/... -> "operations"
            models/search/marts/... -> "search"

        Returns:
            Domain name or None
        """
        from .constants import DOMAINS

        parts = file_path.parts

        for domain in DOMAINS:
            if domain in parts:
                return domain

        return None

    def detect_layer_from_name(self, model_name: str) -> Optional[str]:
        """
        Detect model layer from name prefix

        Examples:
            stg_op_stock -> "staging"
            int_search_base -> "intermediate"
            mart_search_funnel -> "mart"

        Returns:
            Layer name or None
        """
        from .constants import MODEL_LAYERS

        for layer, info in MODEL_LAYERS.items():
            prefix = info["prefix"]
            if model_name.startswith(prefix):
                return layer

        return None
