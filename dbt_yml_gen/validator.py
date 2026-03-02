"""
YAML validator for dbt schema files
"""
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ruamel.yaml import YAML
from ruamel.yaml.scanner import ScannerError
from rich.console import Console
from rich.table import Table

from .constants import (
    DATA_CLASSIFICATIONS,
    ANONYMIZATION_METHODS,
    DOMAINS,
    MATERIALIZATION_TYPES,
    REQUIRED_MODEL_META,
    REQUIRED_COLUMN_META,
    DBT_SCHEMA_VERSION,
    VALIDATION_RULES,
)

console = Console()


class ValidationError:
    """Validation error details"""

    def __init__(
        self,
        severity: str,  # "error", "warning", "info"
        location: str,  # e.g., "model.stg_op_stock.meta.tech_owner"
        message: str,
        suggestion: Optional[str] = None,
    ):
        self.severity = severity
        self.location = location
        self.message = message
        self.suggestion = suggestion

    def __str__(self):
        return f"[{self.severity.upper()}] {self.location}: {self.message}"


class YMLValidator:
    """Validator for dbt YAML schema files"""

    def __init__(self):
        self.yaml = YAML()
        self.errors: List[ValidationError] = []
        self.warnings: List[ValidationError] = []

    def validate_file(self, file_path: Path) -> Tuple[bool, List[ValidationError]]:
        """
        Validate a dbt YAML schema file

        Args:
            file_path: Path to YAML file

        Returns:
            Tuple of (is_valid, list of validation errors/warnings)
        """
        self.errors = []
        self.warnings = []

        if not file_path.exists():
            self.errors.append(
                ValidationError(
                    severity="error",
                    location="file",
                    message=f"File not found: {file_path}",
                )
            )
            return False, self.errors

        try:
            # Load YAML
            with open(file_path, "r", encoding="utf-8") as f:
                data = self.yaml.load(f)

            if not data:
                self.errors.append(
                    ValidationError(
                        severity="error",
                        location="file",
                        message="Empty YAML file",
                    )
                )
                return False, self.errors

            # Validate structure
            self._validate_root_structure(data)

            # Validate models
            if "models" in data:
                for model in data["models"]:
                    self._validate_model(model)

            # Check for errors
            is_valid = len(self.errors) == 0

            return is_valid, self.errors + self.warnings

        except ScannerError as e:
            self.errors.append(
                ValidationError(
                    severity="error",
                    location="file",
                    message=f"YAML syntax error: {str(e)}",
                    suggestion="Check YAML formatting and indentation",
                )
            )
            return False, self.errors

        except Exception as e:
            self.errors.append(
                ValidationError(
                    severity="error",
                    location="file",
                    message=f"Validation error: {str(e)}",
                )
            )
            return False, self.errors

    def _validate_root_structure(self, data: Dict):
        """Validate root-level YAML structure"""
        # Check version
        if "version" not in data:
            self.errors.append(
                ValidationError(
                    severity="error",
                    location="root",
                    message="Missing 'version' field",
                    suggestion=f"Add 'version: {DBT_SCHEMA_VERSION}' at root level",
                )
            )
        elif data["version"] != DBT_SCHEMA_VERSION:
            self.warnings.append(
                ValidationError(
                    severity="warning",
                    location="root.version",
                    message=f"Expected version {DBT_SCHEMA_VERSION}, found {data['version']}",
                )
            )

        # Check models
        if "models" not in data:
            self.errors.append(
                ValidationError(
                    severity="error",
                    location="root",
                    message="Missing 'models' list",
                    suggestion="Add 'models:' with at least one model definition",
                )
            )
        elif not isinstance(data["models"], list):
            self.errors.append(
                ValidationError(
                    severity="error",
                    location="root.models",
                    message="'models' must be a list",
                )
            )

    def _validate_model(self, model: Dict):
        """Validate a single model definition"""
        if not isinstance(model, dict):
            self.errors.append(
                ValidationError(
                    severity="error",
                    location="model",
                    message="Model must be a dictionary",
                )
            )
            return

        # Get model name for better error messages
        model_name = model.get("name", "unknown")
        location_prefix = f"model.{model_name}"

        # Required: name
        if "name" not in model:
            self.errors.append(
                ValidationError(
                    severity="error",
                    location=location_prefix,
                    message="Missing 'name' field",
                )
            )
            return

        # Required: description
        if "description" not in model:
            self.warnings.append(
                ValidationError(
                    severity="warning",
                    location=f"{location_prefix}.description",
                    message="Missing model description",
                    suggestion="Add a description explaining the model's purpose",
                )
            )
        elif not model["description"] or model["description"].strip() == "":
            self.warnings.append(
                ValidationError(
                    severity="warning",
                    location=f"{location_prefix}.description",
                    message="Empty model description",
                )
            )
        else:
            # Check description length
            desc_len = len(model["description"])
            if desc_len < VALIDATION_RULES["min_description_length"]:
                self.warnings.append(
                    ValidationError(
                        severity="warning",
                        location=f"{location_prefix}.description",
                        message=f"Description too short ({desc_len} chars, min {VALIDATION_RULES['min_description_length']})",
                    )
                )

        # Validate config
        if "config" in model:
            self._validate_config(model["config"], location_prefix)

        # Validate meta (required fields)
        if "meta" in model:
            self._validate_model_meta(model["meta"], location_prefix)
        else:
            self.errors.append(
                ValidationError(
                    severity="error",
                    location=f"{location_prefix}.meta",
                    message="Missing 'meta' section",
                    suggestion=f"Add meta with required fields: {', '.join(REQUIRED_MODEL_META)}",
                )
            )

        # Validate columns
        if "columns" in model:
            if not isinstance(model["columns"], list):
                self.errors.append(
                    ValidationError(
                        severity="error",
                        location=f"{location_prefix}.columns",
                        message="'columns' must be a list",
                    )
                )
            else:
                for column in model["columns"]:
                    self._validate_column(column, location_prefix)
        else:
            self.warnings.append(
                ValidationError(
                    severity="warning",
                    location=f"{location_prefix}.columns",
                    message="No columns defined",
                    suggestion="Add column definitions with data types and metadata",
                )
            )

    def _validate_config(self, config: Dict, location_prefix: str):
        """Validate config section"""
        if "contract" in config:
            contract = config["contract"]
            if "enforced" in contract:
                if contract["enforced"] is True:
                    self.warnings.append(
                        ValidationError(
                            severity="warning",
                            location=f"{location_prefix}.config.contract.enforced",
                            message="Contract enforcement is true (should be false for ClickHouse)",
                            suggestion="Set to false - ClickHouse doesn't support runtime enforcement",
                        )
                    )

    def _validate_model_meta(self, meta: Dict, location_prefix: str):
        """Validate model-level meta section"""
        # Check required fields
        for field in REQUIRED_MODEL_META:
            if field not in meta:
                self.warnings.append(
                    ValidationError(
                        severity="warning",
                        location=f"{location_prefix}.meta.{field}",
                        message=f"Missing required meta field: {field}",
                    )
                )
            elif not meta[field] or meta[field] == "":
                self.warnings.append(
                    ValidationError(
                        severity="warning",
                        location=f"{location_prefix}.meta.{field}",
                        message=f"Empty value for required field: {field}",
                        suggestion="Fill in this required field",
                    )
                )

        # Validate field values
        if "data_classification" in meta:
            if meta["data_classification"] not in DATA_CLASSIFICATIONS:
                self.errors.append(
                    ValidationError(
                        severity="error",
                        location=f"{location_prefix}.meta.data_classification",
                        message=f"Invalid data classification: {meta['data_classification']}",
                        suggestion=f"Must be one of: {', '.join(DATA_CLASSIFICATIONS)}",
                    )
                )

        if "domain" in meta:
            if meta["domain"] not in DOMAINS:
                self.warnings.append(
                    ValidationError(
                        severity="warning",
                        location=f"{location_prefix}.meta.domain",
                        message=f"Unknown domain: {meta['domain']}",
                        suggestion=f"Consider using: {', '.join(DOMAINS)}",
                    )
                )

        if "materialization" in meta:
            if meta["materialization"] not in MATERIALIZATION_TYPES:
                self.warnings.append(
                    ValidationError(
                        severity="warning",
                        location=f"{location_prefix}.meta.materialization",
                        message=f"Unknown materialization: {meta['materialization']}",
                        suggestion=f"Should be one of: {', '.join(MATERIALIZATION_TYPES)}",
                    )
                )

        if "pii" in meta:
            if not isinstance(meta["pii"], bool):
                self.errors.append(
                    ValidationError(
                        severity="error",
                        location=f"{location_prefix}.meta.pii",
                        message=f"PII flag must be boolean (true/false), got: {meta['pii']}",
                    )
                )

    def _validate_column(self, column: Dict, model_location: str):
        """Validate a column definition"""
        if not isinstance(column, dict):
            self.errors.append(
                ValidationError(
                    severity="error",
                    location=f"{model_location}.columns",
                    message="Column must be a dictionary",
                )
            )
            return

        # Get column name
        col_name = column.get("name", "unknown")
        location_prefix = f"{model_location}.columns.{col_name}"

        # Required: name
        if "name" not in column:
            self.errors.append(
                ValidationError(
                    severity="error",
                    location=location_prefix,
                    message="Missing column 'name'",
                )
            )

        # Required: description
        if "description" not in column:
            self.warnings.append(
                ValidationError(
                    severity="warning",
                    location=f"{location_prefix}.description",
                    message="Missing column description",
                )
            )
        elif not column["description"] or column["description"].strip() == "":
            self.warnings.append(
                ValidationError(
                    severity="warning",
                    location=f"{location_prefix}.description",
                    message="Empty column description",
                )
            )

        # Required: data_type
        if "data_type" not in column:
            self.warnings.append(
                ValidationError(
                    severity="warning",
                    location=f"{location_prefix}.data_type",
                    message="Missing data_type",
                    suggestion="Add ClickHouse data type (e.g., UInt64, String, DateTime)",
                )
            )

        # Validate meta
        if "meta" in column:
            self._validate_column_meta(column["meta"], location_prefix)
        else:
            self.warnings.append(
                ValidationError(
                    severity="warning",
                    location=f"{location_prefix}.meta",
                    message="Missing column meta",
                    suggestion=f"Add meta with: {', '.join(REQUIRED_COLUMN_META)}",
                )
            )

        # Validate tests
        if "tests" in column:
            self._validate_tests(column["tests"], location_prefix)

    def _validate_column_meta(self, meta: Dict, location_prefix: str):
        """Validate column-level meta section"""
        # Check required fields
        for field in REQUIRED_COLUMN_META:
            if field not in meta:
                self.warnings.append(
                    ValidationError(
                        severity="warning",
                        location=f"{location_prefix}.meta.{field}",
                        message=f"Missing required meta field: {field}",
                    )
                )

        # Validate values
        if "data_classification" in meta:
            if meta["data_classification"] not in DATA_CLASSIFICATIONS:
                self.errors.append(
                    ValidationError(
                        severity="error",
                        location=f"{location_prefix}.meta.data_classification",
                        message=f"Invalid data classification: {meta['data_classification']}",
                        suggestion=f"Must be one of: {', '.join(DATA_CLASSIFICATIONS)}",
                    )
                )

        if "pii" in meta:
            if not isinstance(meta["pii"], bool):
                self.errors.append(
                    ValidationError(
                        severity="error",
                        location=f"{location_prefix}.meta.pii",
                        message=f"PII flag must be boolean, got: {meta['pii']}",
                    )
                )

            # If PII is true, check for anonymization method
            if meta.get("pii") is True:
                if "anonymization_method" not in meta:
                    self.warnings.append(
                        ValidationError(
                            severity="warning",
                            location=f"{location_prefix}.meta.anonymization_method",
                            message="PII column missing anonymization method",
                            suggestion=f"Add anonymization_method: {', '.join(ANONYMIZATION_METHODS)}",
                        )
                    )
                elif meta["anonymization_method"] not in ANONYMIZATION_METHODS:
                    self.errors.append(
                        ValidationError(
                            severity="error",
                            location=f"{location_prefix}.meta.anonymization_method",
                            message=f"Invalid anonymization method: {meta['anonymization_method']}",
                            suggestion=f"Must be one of: {', '.join(ANONYMIZATION_METHODS)}",
                        )
                    )

    def _validate_tests(self, tests: List, location_prefix: str):
        """Validate tests section"""
        if not isinstance(tests, list):
            self.errors.append(
                ValidationError(
                    severity="error",
                    location=f"{location_prefix}.tests",
                    message="Tests must be a list",
                )
            )

    def display_results(self, errors: List[ValidationError]):
        """
        Display validation results in a table

        Args:
            errors: List of ValidationError objects
        """
        if not errors:
            console.print("\n[green]✓ Validation passed with no errors or warnings[/green]\n")
            return

        # Separate by severity
        error_list = [e for e in errors if e.severity == "error"]
        warning_list = [e for e in errors if e.severity == "warning"]

        # Display errors
        if error_list:
            table = Table(title="Validation Errors", border_style="red")
            table.add_column("Location", style="cyan")
            table.add_column("Message", style="red")
            table.add_column("Suggestion", style="yellow")

            for error in error_list:
                table.add_row(
                    error.location,
                    error.message,
                    error.suggestion or "",
                )

            console.print()
            console.print(table)
            console.print()

        # Display warnings
        if warning_list:
            table = Table(title="Validation Warnings", border_style="yellow")
            table.add_column("Location", style="cyan")
            table.add_column("Message", style="yellow")
            table.add_column("Suggestion", style="green")

            for warning in warning_list:
                table.add_row(
                    warning.location,
                    warning.message,
                    warning.suggestion or "",
                )

            console.print()
            console.print(table)
            console.print()

        # Summary
        console.print(
            f"[bold]Summary:[/bold] {len(error_list)} errors, {len(warning_list)} warnings\n"
        )
