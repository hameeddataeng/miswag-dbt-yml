"""
Interactive CLI prompts using Rich library
"""
from typing import Dict, List, Optional

from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.panel import Panel

from .constants import (
    DATA_CLASSIFICATIONS,
    ANONYMIZATION_METHODS,
    DOMAINS,
    MATERIALIZATION_TYPES,
)
from .column_analyzer import ColumnAnalysis

console = Console()


class InteractivePrompt:
    """Interactive prompts for collecting metadata"""

    def __init__(self):
        self.console = console

    def prompt_model_metadata(
        self, model_name: str, defaults: Optional[Dict] = None
    ) -> Dict:
        """
        Prompt user for model-level metadata

        Args:
            model_name: Name of the model
            defaults: Default values to pre-fill

        Returns:
            Dict with model metadata
        """
        defaults = defaults or {}

        self.console.print(
            Panel.fit(
                f"[bold cyan]Model Metadata Collection[/bold cyan]\n"
                f"Model: [yellow]{model_name}[/yellow]",
                border_style="cyan",
            )
        )

        meta = {}

        # Description
        meta["description"] = Prompt.ask(
            "\n[cyan]Model Description[/cyan]",
            default=defaults.get("description", f"{model_name.replace('_', ' ').title()}"),
        )

        # Tech Owner (required)
        meta["tech_owner"] = Prompt.ask(
            "[cyan]Technical Owner[/cyan] (person responsible for model)",
            default=defaults.get("tech_owner", ""),
        )

        # Business Owner (required)
        meta["business_owner"] = Prompt.ask(
            "[cyan]Business Owner[/cyan] (team/person who owns the data)",
            default=defaults.get("business_owner", ""),
        )

        # Data Classification (required)
        meta["data_classification"] = self.prompt_choice(
            "Data Classification",
            DATA_CLASSIFICATIONS,
            default=defaults.get("data_classification", "Internal"),
        )

        # Domain (required)
        meta["domain"] = self.prompt_choice(
            "Business Domain",
            DOMAINS,
            default=defaults.get("domain", "operations"),
        )

        # Source System (required)
        meta["source_system"] = Prompt.ask(
            "[cyan]Source System[/cyan] (e.g., miswagdb, rudderstack)",
            default=defaults.get("source_system", "miswagdb"),
        )

        # Materialization (required)
        meta["materialization"] = self.prompt_choice(
            "Materialization Type",
            MATERIALIZATION_TYPES,
            default=defaults.get("materialization", "view"),
        )

        # Optional: Data Loading
        if Confirm.ask("[cyan]Add data loading method?[/cyan]", default=True):
            meta["data_loading"] = Prompt.ask(
                "  Data Loading Method (e.g., mageai, rudderstack)",
                default=defaults.get("data_loading", "mageai"),
            )

        # Optional: Source Frequency
        if Confirm.ask("[cyan]Add source update frequency?[/cyan]", default=True):
            meta["source_frequency"] = Prompt.ask(
                "  Source Update Frequency (e.g., 24 hours, real-time)",
                default=defaults.get("source_frequency", "24 hours"),
            )

        # Optional: Update Frequency
        if Confirm.ask("[cyan]Add model update frequency?[/cyan]", default=True):
            meta["update_frequency"] = Prompt.ask(
                "  Model Update Frequency (e.g., daily, on_demand)",
                default=defaults.get("update_frequency", "on_demand"),
            )

        # Optional: SLA Hours
        if Confirm.ask("[cyan]Add SLA in hours?[/cyan]", default=False):
            sla_input = Prompt.ask("  SLA Hours", default="24")
            meta["sla_hours"] = int(sla_input)

        self.console.print("\n[green]✓[/green] Model metadata collected\n")

        return meta

    def prompt_choice(
        self,
        prompt_text: str,
        choices: List[str],
        default: Optional[str] = None,
    ) -> str:
        """
        Prompt user to select from a list of choices

        Args:
            prompt_text: Prompt message
            choices: List of valid choices
            default: Default choice

        Returns:
            Selected choice
        """
        # Display choices
        choices_str = ", ".join(f"[yellow]{c}[/yellow]" for c in choices)
        self.console.print(f"[cyan]{prompt_text}[/cyan]: {choices_str}")

        while True:
            choice = Prompt.ask("  Select", default=default or choices[0])

            if choice.lower() in [c.lower() for c in choices]:
                # Find matching choice with correct case
                for c in choices:
                    if c.lower() == choice.lower():
                        return c
            else:
                self.console.print(
                    f"[red]Invalid choice.[/red] Please select from: {', '.join(choices)}"
                )

    def prompt_column_metadata(
        self, analysis: ColumnAnalysis, skip_description: bool = False
    ) -> Dict:
        """
        Prompt user for column-level metadata

        Args:
            analysis: ColumnAnalysis object with smart defaults
            skip_description: Skip description prompt (use template)

        Returns:
            Dict with column metadata
        """
        meta = {}

        self.console.print(f"\n[cyan]Column:[/cyan] [yellow]{analysis.name}[/yellow]")

        # Description
        if not skip_description:
            meta["description"] = Prompt.ask(
                "  Description",
                default=analysis.description_template or "",
            )
        else:
            meta["description"] = analysis.description_template or ""

        # Data classification (use smart default)
        if Confirm.ask(
            f"  Data Classification: [yellow]{analysis.data_classification}[/yellow] (change?)",
            default=False,
        ):
            meta["data_classification"] = self.prompt_choice(
                "  New Data Classification",
                DATA_CLASSIFICATIONS,
                default=analysis.data_classification,
            )
        else:
            meta["data_classification"] = analysis.data_classification

        # PII handling
        if analysis.contains_pii:
            self.console.print(
                f"  [yellow]⚠ PII detected:[/yellow] {analysis.pii_type}"
            )

            # Confirm PII
            meta["pii"] = Confirm.ask(
                f"  Confirm PII in column?",
                default=True,
            )

            if meta["pii"]:
                # Anonymization method
                self.console.print(
                    f"  Recommended: [yellow]{analysis.recommended_anonymization}[/yellow]"
                )

                meta["anonymization_method"] = self.prompt_choice(
                    "  Anonymization Method",
                    ANONYMIZATION_METHODS,
                    default=analysis.recommended_anonymization,
                )
            else:
                meta["anonymization_method"] = "none"
        else:
            meta["pii"] = False
            meta["anonymization_method"] = "none"

        return meta

    def confirm_pii_summary(self, pii_summary: Dict) -> bool:
        """
        Display PII summary and ask for confirmation

        Args:
            pii_summary: PII summary dict from analyzer

        Returns:
            True if user confirms, False otherwise
        """
        if pii_summary["pii_columns"] == 0:
            self.console.print("\n[green]✓[/green] No PII detected\n")
            return True

        # Display PII table
        table = Table(title="PII Columns Detected", border_style="yellow")
        table.add_column("Column Name", style="cyan")
        table.add_column("PII Type", style="yellow")
        table.add_column("Recommended Anonymization", style="green")

        for detail in pii_summary["pii_details"]:
            table.add_row(
                detail["name"],
                detail["type"],
                detail["recommended_anonymization"],
            )

        self.console.print()
        self.console.print(table)
        self.console.print()

        self.console.print(
            f"[yellow]⚠ Warning:[/yellow] {pii_summary['pii_columns']} "
            f"of {pii_summary['total_columns']} columns contain PII "
            f"({pii_summary['pii_percentage']:.1f}%)"
        )

        return Confirm.ask("\nContinue with PII columns?", default=True)

    def display_column_summary(self, analyses: List[ColumnAnalysis]):
        """
        Display summary table of columns

        Args:
            analyses: List of ColumnAnalysis objects
        """
        table = Table(title="Column Summary", border_style="cyan")
        table.add_column("Column Name", style="cyan")
        table.add_column("Type", style="green")
        table.add_column("Nullable", style="yellow")
        table.add_column("PK", justify="center")
        table.add_column("FK", justify="center")
        table.add_column("PII", justify="center")
        table.add_column("Classification", style="magenta")

        for analysis in analyses:
            table.add_row(
                analysis.name,
                analysis.data_type,
                "✓" if analysis.is_nullable else "✗",
                "✓" if analysis.is_primary_key else "",
                "✓" if analysis.is_foreign_key else "",
                "⚠" if analysis.contains_pii else "",
                analysis.data_classification,
            )

        self.console.print()
        self.console.print(table)
        self.console.print()

    def prompt_batch_mode(self) -> str:
        """
        Ask user if they want to process columns in batch or individually

        Returns:
            "batch", "individual", or "skip"
        """
        self.console.print("\n[cyan]Column Metadata Collection Mode:[/cyan]")
        self.console.print("  [yellow]1.[/yellow] Batch - Use smart defaults for all columns (fastest)")
        self.console.print("  [yellow]2.[/yellow] Individual - Review and edit each column")
        self.console.print("  [yellow]3.[/yellow] Skip - Only use smart defaults, no prompts")

        choice = Prompt.ask("Select mode", choices=["1", "2", "3"], default="1")

        if choice == "1":
            return "batch"
        elif choice == "2":
            return "individual"
        else:
            return "skip"

    def prompt_tags(self, suggested_tags: Optional[List[str]] = None) -> List[str]:
        """
        Prompt user for model tags

        Args:
            suggested_tags: Pre-populated tag suggestions

        Returns:
            List of tags
        """
        suggested_tags = suggested_tags or []

        self.console.print(f"\n[cyan]Tags[/cyan] (comma-separated)")
        if suggested_tags:
            self.console.print(f"  Suggested: [yellow]{', '.join(suggested_tags)}[/yellow]")

        tags_input = Prompt.ask(
            "  Tags",
            default=", ".join(suggested_tags) if suggested_tags else "",
        )

        if not tags_input:
            return []

        # Parse comma-separated tags
        tags = [tag.strip() for tag in tags_input.split(",") if tag.strip()]

        return tags

    def display_success(self, output_path: str):
        """
        Display success message with output path

        Args:
            output_path: Path to generated YAML file
        """
        self.console.print(
            Panel.fit(
                f"[bold green]✓ YAML Generated Successfully[/bold green]\n\n"
                f"Output: [cyan]{output_path}[/cyan]",
                border_style="green",
            )
        )

    def display_error(self, error_message: str):
        """
        Display error message

        Args:
            error_message: Error message to display
        """
        self.console.print(
            Panel.fit(
                f"[bold red]✗ Error[/bold red]\n\n{error_message}",
                border_style="red",
            )
        )

    def confirm_overwrite(self, file_path: str) -> bool:
        """
        Ask user to confirm file overwrite

        Args:
            file_path: Path to existing file

        Returns:
            True if user confirms overwrite, False otherwise
        """
        self.console.print(f"\n[yellow]⚠ File exists:[/yellow] {file_path}")

        return Confirm.ask("Overwrite existing file?", default=False)
