"""
YAML generator for dbt schema files using ruamel.yaml
"""
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from rich.console import Console

from .constants import (
    DBT_SCHEMA_VERSION,
    CONTRACT_ENFORCED,
    YAML_FORMATTING,
)
from .column_analyzer import ColumnAnalysis

console = Console()


class YMLGenerator:
    """Generator for dbt YAML schema files"""

    def __init__(self):
        self.yaml = YAML()
        self.yaml.indent(
            mapping=YAML_FORMATTING["indent"],
            sequence=YAML_FORMATTING["indent"],
            offset=YAML_FORMATTING["block_seq_indent"],
        )
        self.yaml.width = YAML_FORMATTING["line_width"]
        self.yaml.preserve_quotes = YAML_FORMATTING["preserve_quotes"]

    def generate_model_yml(
        self,
        model_name: str,
        description: str,
        columns: List[ColumnAnalysis],
        meta: Dict,
        tags: Optional[List[str]] = None,
        tests: Optional[List] = None,
    ) -> CommentedMap:
        """
        Generate complete model YAML structure

        Args:
            model_name: Name of the dbt model
            description: Model description
            columns: List of ColumnAnalysis objects
            meta: Metadata dictionary
            tags: List of tags
            tests: List of model-level tests

        Returns:
            CommentedMap (ordered dict) for YAML generation
        """
        model = CommentedMap()

        # Model name
        model["name"] = model_name

        # Description
        model["description"] = description

        # Tags
        if tags:
            model["tags"] = CommentedSeq(tags)

        # Config with contract enforcement
        model["config"] = CommentedMap()
        model["config"]["contract"] = CommentedMap()
        model["config"]["contract"]["enforced"] = CONTRACT_ENFORCED

        # Add comment for contract enforcement
        model["config"]["contract"].yaml_add_eol_comment(
            "Always false for ClickHouse - tests are the only enforcement layer",
            "enforced",
        )

        # Meta
        model["meta"] = self._build_meta_dict(meta)

        # Model-level tests
        if tests:
            model["tests"] = CommentedSeq(tests)

        # Columns
        model["columns"] = self._build_columns_list(columns)

        return model

    def _build_meta_dict(self, meta: Dict) -> CommentedMap:
        """
        Build meta dictionary with proper ordering

        Args:
            meta: Metadata dictionary

        Returns:
            CommentedMap with ordered metadata
        """
        meta_dict = CommentedMap()

        # Ownership
        if "tech_owner" in meta:
            meta_dict["tech_owner"] = meta["tech_owner"]
        if "business_owner" in meta:
            meta_dict["business_owner"] = meta["business_owner"]

        # Classification
        if "data_classification" in meta:
            meta_dict["data_classification"] = meta["data_classification"]

        # Source info
        if "source_system" in meta:
            meta_dict["source_system"] = meta["source_system"]
        if "data_loading" in meta:
            meta_dict["data_loading"] = meta["data_loading"]
        if "source_frequency" in meta:
            meta_dict["source_frequency"] = meta["source_frequency"]

        # Model info
        if "materialization" in meta:
            meta_dict["materialization"] = meta["materialization"]
        if "update_frequency" in meta:
            meta_dict["update_frequency"] = meta["update_frequency"]

        # Governance
        if "pii" in meta:
            meta_dict["pii"] = meta["pii"]
        if "domain" in meta:
            meta_dict["domain"] = meta["domain"]
        if "sla_hours" in meta:
            meta_dict["sla_hours"] = meta["sla_hours"]

        # Additional fields
        for key, value in meta.items():
            if key not in meta_dict:
                meta_dict[key] = value

        return meta_dict

    def _build_columns_list(self, columns: List[ColumnAnalysis]) -> CommentedSeq:
        """
        Build columns list from ColumnAnalysis objects

        Args:
            columns: List of ColumnAnalysis objects

        Returns:
            CommentedSeq (ordered list) of column definitions
        """
        columns_list = CommentedSeq()

        for col in columns:
            col_dict = CommentedMap()

            # Column name
            col_dict["name"] = col.name

            # Description
            col_dict["description"] = col.description_template or ""

            # Add comment if description is empty
            if not col.description_template:
                col_dict.yaml_add_eol_comment(
                    "TODO: Add description",
                    "description",
                )

            # Data type
            col_dict["data_type"] = col.data_type

            # Tests
            tests = self._get_column_tests(col)
            if tests:
                col_dict["tests"] = CommentedSeq(tests)

            # Meta
            col_dict["meta"] = self._build_column_meta(col)

            columns_list.append(col_dict)

        return columns_list

    def _get_column_tests(self, col: ColumnAnalysis) -> List:
        """
        Get dbt tests for a column based on analysis

        Args:
            col: ColumnAnalysis object

        Returns:
            List of test definitions
        """
        tests = []

        # Primary keys: not_null + unique
        if col.is_primary_key:
            tests.append("not_null")
            if not any(
                other_test == "unique" for other_test in tests
            ):  # Avoid duplicates
                tests.append("unique")

        # Foreign keys: not_null (relationships requires ref info)
        elif col.is_foreign_key:
            tests.append("not_null")

        # Non-nullable columns
        elif not col.is_nullable:
            tests.append("not_null")

        return tests

    def _build_column_meta(self, col: ColumnAnalysis) -> CommentedMap:
        """
        Build column-level meta dictionary

        Args:
            col: ColumnAnalysis object

        Returns:
            CommentedMap with column metadata
        """
        meta = CommentedMap()

        # Data classification
        meta["data_classification"] = col.data_classification

        # PII flag
        meta["pii"] = col.contains_pii

        # Anonymization method (if PII)
        if col.contains_pii and col.recommended_anonymization:
            meta["anonymization_method"] = col.recommended_anonymization

            # Add comment with warning
            meta.yaml_add_eol_comment(
                "⚠ PII detected - review anonymization method",
                "pii",
            )

        return meta

    def write_yml_file(
        self,
        file_path: Path,
        models: List[CommentedMap],
        overwrite: bool = False,
    ) -> bool:
        """
        Write YAML file with model definitions

        Args:
            file_path: Output file path
            models: List of model CommentedMap objects
            overwrite: Whether to overwrite existing file

        Returns:
            True if successful, False otherwise
        """
        # Check if file exists
        if file_path.exists() and not overwrite:
            # Create file with suffix
            suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = file_path.with_stem(f"{file_path.stem}_{suffix}")
            console.print(
                f"[yellow]⚠[/yellow] File exists, creating: {file_path.name}"
            )

        try:
            # Build root structure
            root = CommentedMap()
            root["version"] = DBT_SCHEMA_VERSION
            root["models"] = CommentedSeq(models)

            # Write to file
            with open(file_path, "w", encoding="utf-8") as f:
                self.yaml.dump(root, f)

            console.print(f"[green]✓[/green] YAML file created: {file_path}")
            return True

        except Exception as e:
            console.print(f"[red]Error:[/red] Failed to write YAML file: {str(e)}")
            return False

    def generate_from_template(
        self,
        model_name: str,
        columns: List[ColumnAnalysis],
        domain: Optional[str] = None,
        layer: Optional[str] = None,
    ) -> CommentedMap:
        """
        Generate model YAML from template with smart defaults

        Args:
            model_name: Name of the model
            columns: List of ColumnAnalysis objects
            domain: Domain name (for default values)
            layer: Model layer (staging, intermediate, mart)

        Returns:
            CommentedMap for model definition
        """
        from .constants import DOMAIN_DEFAULTS, MODEL_LAYERS

        # Build meta with defaults
        meta = CommentedMap()

        # Add domain-specific defaults
        if domain and domain in DOMAIN_DEFAULTS:
            defaults = DOMAIN_DEFAULTS[domain]
            for key, value in defaults.items():
                meta[key] = value

        # Add layer-specific defaults
        if layer and layer in MODEL_LAYERS:
            meta["materialization"] = MODEL_LAYERS[layer]["materialization"]

        # Add tech owner (required - needs to be filled in)
        if "tech_owner" not in meta:
            meta["tech_owner"] = ""  # Will be commented

        # Add data classification
        if "data_classification" not in meta:
            meta["data_classification"] = "Internal"

        # Check for PII in columns
        has_pii = any(col.contains_pii for col in columns)
        meta["pii"] = has_pii

        # Add domain
        if domain:
            meta["domain"] = domain

        # Build description template
        description = f"{model_name.replace('_', ' ').title()}"

        # Build tags
        tags = []
        if layer:
            tags.append(layer)
        if domain:
            tags.append(domain)

        # Generate model
        model = self.generate_model_yml(
            model_name=model_name,
            description=description,
            columns=columns,
            meta=meta,
            tags=tags if tags else None,
        )

        # Add comments for empty required fields
        if not meta.get("tech_owner"):
            model["meta"].yaml_add_eol_comment(
                "TODO: Add technical owner",
                "tech_owner",
            )

        return model

    def add_header_comment(self, root: CommentedMap, comment: str):
        """
        Add header comment to YAML file

        Args:
            root: Root CommentedMap object
            comment: Comment text
        """
        root.yaml_set_start_comment(comment)
