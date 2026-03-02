"""
Column analyzer for smart defaults and PII detection
"""
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from rich.console import Console

from .constants import (
    COLUMN_PATTERNS,
    PII_PATTERNS,
    DATA_CLASSIFICATIONS,
    ANONYMIZATION_METHODS,
)

console = Console()


@dataclass
class ColumnAnalysis:
    """Analysis results for a single column"""

    name: str
    data_type: str
    is_nullable: bool

    # Smart defaults
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_timestamp: bool = False
    is_flag: bool = False
    is_count: bool = False
    is_amount: bool = False

    # PII detection
    contains_pii: bool = False
    pii_type: Optional[str] = None
    recommended_anonymization: Optional[str] = None

    # Governance
    data_classification: str = "Internal"
    description_template: Optional[str] = None


class ColumnAnalyzer:
    """Analyzer for column metadata and smart defaults"""

    def __init__(self):
        pass

    def analyze_column(
        self, column_name: str, data_type: str, is_nullable: bool = False
    ) -> ColumnAnalysis:
        """
        Analyze a column and generate smart defaults

        Args:
            column_name: Name of the column
            data_type: ClickHouse data type
            is_nullable: Whether column is nullable

        Returns:
            ColumnAnalysis object with recommendations
        """
        analysis = ColumnAnalysis(
            name=column_name, data_type=data_type, is_nullable=is_nullable
        )

        # Detect column patterns
        self._detect_column_patterns(analysis)

        # Detect PII
        self._detect_pii(analysis)

        # Set data classification
        self._set_data_classification(analysis)

        # Generate description template
        self._generate_description_template(analysis)

        return analysis

    def _detect_column_patterns(self, analysis: ColumnAnalysis):
        """
        Detect column patterns based on name

        Updates analysis object with pattern flags
        """
        name_lower = analysis.name.lower()

        # Check primary key patterns
        for pattern in COLUMN_PATTERNS["is_primary_key"]:
            if re.match(pattern, name_lower):
                analysis.is_primary_key = True
                break

        # Check foreign key patterns
        for pattern in COLUMN_PATTERNS["is_foreign_key"]:
            if re.match(pattern, name_lower):
                analysis.is_foreign_key = True
                break

        # Check timestamp patterns
        for pattern in COLUMN_PATTERNS["is_timestamp"]:
            if re.match(pattern, name_lower):
                analysis.is_timestamp = True
                break

        # Check flag patterns
        for pattern in COLUMN_PATTERNS["is_flag"]:
            if re.match(pattern, name_lower):
                analysis.is_flag = True
                break

        # Check count patterns
        for pattern in COLUMN_PATTERNS["is_count"]:
            if re.match(pattern, name_lower):
                analysis.is_count = True
                break

        # Check amount patterns
        for pattern in COLUMN_PATTERNS["is_amount"]:
            if re.match(pattern, name_lower):
                analysis.is_amount = True
                break

    def _detect_pii(self, analysis: ColumnAnalysis):
        """
        Detect PII in column name

        Updates analysis object with PII flags and recommendations
        """
        name_lower = analysis.name.lower()

        for pattern, anonymization_method in PII_PATTERNS.items():
            if re.search(pattern, name_lower):
                analysis.contains_pii = True
                analysis.pii_type = pattern
                analysis.recommended_anonymization = anonymization_method

                console.print(
                    f"[yellow]⚠ PII detected:[/yellow] {analysis.name} "
                    f"(recommended: {anonymization_method})"
                )
                break

    def _set_data_classification(self, analysis: ColumnAnalysis):
        """
        Set data classification based on column analysis

        Priority:
        1. PII data -> "PII"
        2. Primary/Foreign keys -> "Internal"
        3. Amounts/Sensitive -> "Sensitive"
        4. Default -> "Internal"
        """
        if analysis.contains_pii:
            analysis.data_classification = "PII"
        elif analysis.is_primary_key or analysis.is_foreign_key:
            analysis.data_classification = "Internal"
        elif analysis.is_amount:
            # Amounts might be sensitive depending on context
            analysis.data_classification = "Sensitive"
        else:
            analysis.data_classification = "Internal"

    def _generate_description_template(self, analysis: ColumnAnalysis):
        """
        Generate a description template based on column type

        Provides helpful starting points for documentation
        """
        name = analysis.name

        if analysis.is_primary_key:
            analysis.description_template = (
                f"Unique identifier for {self._humanize_name(name)}"
            )

        elif analysis.is_foreign_key:
            # Try to infer referenced entity
            ref_entity = name.replace("_id", "").replace("_fk", "").replace("_ref", "")
            analysis.description_template = (
                f"Foreign key reference to {self._humanize_name(ref_entity)}"
            )

        elif analysis.is_timestamp:
            if "created" in name.lower():
                analysis.description_template = (
                    f"Timestamp when record was created"
                )
            elif "updated" in name.lower():
                analysis.description_template = (
                    f"Timestamp when record was last updated"
                )
            elif "deleted" in name.lower():
                analysis.description_template = f"Timestamp when record was deleted"
            else:
                analysis.description_template = (
                    f"Timestamp for {self._humanize_name(name)}"
                )

        elif analysis.is_flag:
            analysis.description_template = (
                f"Boolean flag indicating {self._humanize_name(name)}"
            )

        elif analysis.is_count:
            entity = name.replace("_count", "").replace("_cnt", "").replace("total_", "")
            analysis.description_template = (
                f"Count of {self._humanize_name(entity)}"
            )

        elif analysis.is_amount:
            entity = (
                name.replace("_amount", "")
                .replace("_value", "")
                .replace("_price", "")
                .replace("_cost", "")
            )
            analysis.description_template = (
                f"Amount/value for {self._humanize_name(entity)}"
            )

        else:
            analysis.description_template = f"{self._humanize_name(name)}"

    def _humanize_name(self, name: str) -> str:
        """
        Convert snake_case to human-readable format

        Examples:
            user_id -> user ID
            first_name -> first name
            created_at -> created at
        """
        # Replace underscores with spaces
        humanized = name.replace("_", " ")

        # Capitalize appropriately (keep ID, UTC, etc. uppercase)
        words = humanized.split()
        result = []

        for word in words:
            if word.upper() in ["ID", "UTC", "AST", "URL", "API", "PII"]:
                result.append(word.upper())
            else:
                result.append(word.lower())

        return " ".join(result)

    def detect_composite_key(
        self, analyses: List[ColumnAnalysis]
    ) -> Optional[List[str]]:
        """
        Detect if columns form a composite primary key

        Looks for multiple columns marked as is_primary_key

        Args:
            analyses: List of ColumnAnalysis objects

        Returns:
            List of column names forming composite key, or None
        """
        pk_columns = [a.name for a in analyses if a.is_primary_key]

        if len(pk_columns) > 1:
            console.print(
                f"[green]✓[/green] Detected composite key: {', '.join(pk_columns)}"
            )
            return pk_columns

        return None

    def generate_tests_for_column(self, analysis: ColumnAnalysis) -> List[str]:
        """
        Generate recommended dbt tests for a column

        Args:
            analysis: ColumnAnalysis object

        Returns:
            List of test names
        """
        tests = []

        # Primary keys should be unique and not null
        if analysis.is_primary_key:
            tests.append("not_null")
            tests.append("unique")

        # Foreign keys should not be null (usually)
        elif analysis.is_foreign_key:
            tests.append("not_null")
            # Note: relationships test requires target model info

        # Non-nullable columns
        elif not analysis.is_nullable:
            tests.append("not_null")

        # Flags should have accepted values (0, 1 or true, false)
        if analysis.is_flag:
            # This would need to be added with specific values
            pass

        return tests

    def batch_analyze(
        self, columns: List[Tuple[str, str, bool]]
    ) -> List[ColumnAnalysis]:
        """
        Analyze multiple columns at once

        Args:
            columns: List of (name, data_type, is_nullable) tuples

        Returns:
            List of ColumnAnalysis objects
        """
        analyses = []

        for name, data_type, is_nullable in columns:
            analysis = self.analyze_column(name, data_type, is_nullable)
            analyses.append(analysis)

        # Check for composite keys
        self.detect_composite_key(analyses)

        return analyses

    def get_pii_summary(self, analyses: List[ColumnAnalysis]) -> Dict:
        """
        Get summary of PII columns detected

        Args:
            analyses: List of ColumnAnalysis objects

        Returns:
            Dict with PII summary
        """
        pii_columns = [a for a in analyses if a.contains_pii]

        summary = {
            "total_columns": len(analyses),
            "pii_columns": len(pii_columns),
            "pii_percentage": (
                len(pii_columns) / len(analyses) * 100 if analyses else 0
            ),
            "pii_details": [
                {
                    "name": a.name,
                    "type": a.pii_type,
                    "recommended_anonymization": a.recommended_anonymization,
                }
                for a in pii_columns
            ],
        }

        if pii_columns:
            console.print(
                f"\n[yellow]⚠ PII Summary:[/yellow] {len(pii_columns)} "
                f"of {len(analyses)} columns contain PII "
                f"({summary['pii_percentage']:.1f}%)\n"
            )

        return summary
