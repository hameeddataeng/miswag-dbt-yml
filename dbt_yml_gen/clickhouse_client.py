"""
ClickHouse client for schema introspection
"""
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from rich.console import Console

from .constants import CLICKHOUSE_TYPES

console = Console()


@dataclass
class ColumnSchema:
    """Schema information for a single column"""

    name: str
    data_type: str
    is_nullable: bool
    default_value: Optional[str] = None
    comment: Optional[str] = None


class ClickHouseClient:
    """Client for connecting to ClickHouse and retrieving schema information"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "default",
    ):
        """Initialize ClickHouse client"""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.client: Optional[Client] = None

    def connect(self) -> bool:
        """
        Connect to ClickHouse server

        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
            )
            # Test connection
            self.client.command("SELECT 1")
            console.print(
                f"[green]✓[/green] Connected to ClickHouse at {self.host}:{self.port}"
            )
            return True
        except Exception as e:
            console.print(
                f"[yellow]⚠[/yellow] Could not connect to ClickHouse: {str(e)}"
            )
            return False

    def disconnect(self):
        """Disconnect from ClickHouse server"""
        if self.client:
            self.client.close()
            self.client = None

    def get_table_schema(
        self, table_name: str, database: Optional[str] = None
    ) -> Optional[List[ColumnSchema]]:
        """
        Get schema information for a table

        Args:
            table_name: Name of the table
            database: Database name (uses default if not specified)

        Returns:
            List of ColumnSchema objects, or None if table not found
        """
        if not self.client:
            console.print("[red]Error:[/red] Not connected to ClickHouse")
            return None

        db = database or self.database

        try:
            # Query system.columns for table schema
            query = """
                SELECT
                    name,
                    type,
                    default_expression,
                    comment
                FROM system.columns
                WHERE database = %(database)s
                    AND table = %(table)s
                ORDER BY position
            """

            result = self.client.query(
                query, parameters={"database": db, "table": table_name}
            )

            if not result.result_rows:
                console.print(
                    f"[yellow]⚠[/yellow] Table {db}.{table_name} not found"
                )
                return None

            columns = []
            for row in result.result_rows:
                name, data_type, default_expr, comment = row

                # Normalize data type (remove Nullable wrapper, extract base type)
                normalized_type, is_nullable = self._normalize_type(data_type)

                columns.append(
                    ColumnSchema(
                        name=name,
                        data_type=normalized_type,
                        is_nullable=is_nullable,
                        default_value=default_expr if default_expr else None,
                        comment=comment if comment else None,
                    )
                )

            console.print(
                f"[green]✓[/green] Retrieved schema for {db}.{table_name} ({len(columns)} columns)"
            )
            return columns

        except Exception as e:
            console.print(f"[red]Error:[/red] Failed to get table schema: {str(e)}")
            return None

    def _normalize_type(self, data_type: str) -> Tuple[str, bool]:
        """
        Normalize ClickHouse data type

        Removes Nullable wrapper and returns base type with nullability flag

        Args:
            data_type: Raw ClickHouse data type string

        Returns:
            Tuple of (normalized_type, is_nullable)

        Examples:
            "Nullable(UInt64)" -> ("UInt64", True)
            "UInt64" -> ("UInt64", False)
            "Array(String)" -> ("Array(String)", False)
            "Nullable(DateTime64(3))" -> ("DateTime64(3)", True)
        """
        is_nullable = False

        # Check if type is wrapped in Nullable
        nullable_match = re.match(r"Nullable\((.*)\)", data_type)
        if nullable_match:
            is_nullable = True
            data_type = nullable_match.group(1)

        # Handle LowCardinality wrapper
        lowcard_match = re.match(r"LowCardinality\((.*)\)", data_type)
        if lowcard_match:
            data_type = lowcard_match.group(1)

        return data_type, is_nullable

    def test_connection(self) -> bool:
        """
        Test connection to ClickHouse

        Returns:
            bool: True if connection works, False otherwise
        """
        if not self.client:
            return False

        try:
            self.client.command("SELECT 1")
            return True
        except Exception:
            return False

    def list_tables(self, database: Optional[str] = None) -> List[str]:
        """
        List all tables in a database

        Args:
            database: Database name (uses default if not specified)

        Returns:
            List of table names
        """
        if not self.client:
            console.print("[red]Error:[/red] Not connected to ClickHouse")
            return []

        db = database or self.database

        try:
            query = """
                SELECT name
                FROM system.tables
                WHERE database = %(database)s
                ORDER BY name
            """

            result = self.client.query(query, parameters={"database": db})
            return [row[0] for row in result.result_rows]

        except Exception as e:
            console.print(f"[red]Error:[/red] Failed to list tables: {str(e)}")
            return []

    def get_table_comment(
        self, table_name: str, database: Optional[str] = None
    ) -> Optional[str]:
        """
        Get table-level comment/description

        Args:
            table_name: Name of the table
            database: Database name (uses default if not specified)

        Returns:
            Table comment or None
        """
        if not self.client:
            return None

        db = database or self.database

        try:
            query = """
                SELECT comment
                FROM system.tables
                WHERE database = %(database)s
                    AND name = %(table)s
            """

            result = self.client.query(
                query, parameters={"database": db, "table": table_name}
            )

            if result.result_rows:
                return result.result_rows[0][0] if result.result_rows[0][0] else None

            return None

        except Exception:
            return None
