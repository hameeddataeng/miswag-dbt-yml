"""
CLI commands for miswag-dbt-yml
"""
from pathlib import Path
from typing import Optional

import click
from rich.console import Console

from .clickhouse_client import ClickHouseClient
from .sql_parser import SQLParser
from .column_analyzer import ColumnAnalyzer
from .yml_generator import YMLGenerator
from .interactive import InteractivePrompt
from .validator import YMLValidator
from .config import Config

console = Console()


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """
    miswag-dbt-yml: Automated dbt YAML schema generator with data governance

    Generate standardized dbt YAML schema files for ClickHouse models with
    smart defaults, PII detection, and comprehensive data governance metadata.
    """
    pass


@cli.command()
@click.argument("model_path", type=click.Path(exists=True))
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Output YAML file path (defaults to <model_name>.yml)",
)
@click.option(
    "--skip-clickhouse",
    is_flag=True,
    help="Skip ClickHouse connection and use SQL parsing only",
)
@click.option(
    "--non-interactive",
    is_flag=True,
    help="Non-interactive mode with smart defaults",
)
@click.option(
    "--config",
    type=click.Path(exists=True),
    help="Path to configuration file (defaults to ./defaults.yml)",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite existing YAML file",
)
def generate(
    model_path: str,
    output: Optional[str],
    skip_clickhouse: bool,
    non_interactive: bool,
    config: Optional[str],
    overwrite: bool,
):
    """
    Generate dbt YAML schema file for a model

    EXAMPLE:
        miswag-dbt-yml generate models/staging/stg_op_stock.sql

        miswag-dbt-yml generate models/staging/stg_op_stock.sql --skip-clickhouse

        miswag-dbt-yml generate models/staging/stg_op_stock.sql --non-interactive
    """
    console.print(
        "\n[bold cyan]miswag-dbt-yml:[/bold cyan] YAML Schema Generator\n"
    )

    try:
        # Load configuration
        config_obj = Config(Path(config) if config else None)

        # Parse SQL file
        console.print("[cyan]Step 1:[/cyan] Parsing SQL file...")
        parser = SQLParser()
        model_path_obj = Path(model_path)
        dbt_model = parser.parse_model_file(model_path_obj)

        if not dbt_model:
            console.print("[red]✗ Failed to parse SQL file[/red]")
            return

        # Detect domain and layer
        domain = parser.detect_domain_from_path(model_path_obj)
        layer = parser.detect_layer_from_name(dbt_model.name)

        console.print(f"  Model: [yellow]{dbt_model.name}[/yellow]")
        if domain:
            console.print(f"  Domain: [yellow]{domain}[/yellow]")
        if layer:
            console.print(f"  Layer: [yellow]{layer}[/yellow]")

        # Get column schema
        columns_schema = None

        if not skip_clickhouse:
            console.print("\n[cyan]Step 2:[/cyan] Connecting to ClickHouse...")

            ch_config = config_obj.get_clickhouse_config()
            ch_client = ClickHouseClient(**ch_config)

            if ch_client.connect():
                # Try to get table schema
                # Determine table name (could be from source or model name)
                table_name = dbt_model.name

                columns_schema = ch_client.get_table_schema(table_name)

                if not columns_schema:
                    console.print(
                        f"[yellow]⚠[/yellow] Table {table_name} not found in ClickHouse"
                    )
                    console.print("  Using SQL parsing for column detection")

                ch_client.disconnect()
            else:
                console.print(
                    "[yellow]⚠[/yellow] Could not connect to ClickHouse, using SQL parsing"
                )
        else:
            console.print("\n[cyan]Step 2:[/cyan] Skipping ClickHouse (--skip-clickhouse)")

        # Analyze columns
        console.print("\n[cyan]Step 3:[/cyan] Analyzing columns...")
        analyzer = ColumnAnalyzer()

        if columns_schema:
            # Use ClickHouse schema
            column_analyses = analyzer.batch_analyze(
                [(col.name, col.data_type, col.is_nullable) for col in columns_schema]
            )
        elif dbt_model.columns:
            # Use SQL parsing results (no types)
            column_analyses = analyzer.batch_analyze(
                [(col, "String", False) for col in dbt_model.columns]
            )
        else:
            console.print("[red]✗ No columns found[/red]")
            return

        # Get PII summary
        pii_summary = analyzer.get_pii_summary(column_analyses)

        # Interactive or non-interactive mode
        if non_interactive:
            console.print("\n[cyan]Step 4:[/cyan] Using smart defaults (non-interactive mode)")

            # Get defaults from config
            defaults = config_obj.get_domain_defaults(domain) if domain else config_obj.get_defaults()

            # Build meta with defaults
            meta = {
                "tech_owner": defaults.get("tech_owner", ""),
                "business_owner": defaults.get("business_owner", ""),
                "data_classification": defaults.get("data_classification", "internal"),
                "source_system": defaults.get("source_system", ""),
                "data_loading": defaults.get("data_loading", ""),
                "materialization": dbt_model.materialization or defaults.get("materialization", "view"),
                "pii": pii_summary["pii_columns"] > 0,
                "domain": domain or "",
            }

        else:
            console.print("\n[cyan]Step 4:[/cyan] Collecting metadata (interactive mode)")

            prompt = InteractivePrompt()

            # Display column summary
            prompt.display_column_summary(column_analyses)

            # Confirm PII
            if pii_summary["pii_columns"] > 0:
                if not prompt.confirm_pii_summary(pii_summary):
                    console.print("[yellow]Aborted by user[/yellow]")
                    return

            # Get domain defaults
            defaults = config_obj.get_domain_defaults(domain) if domain else config_obj.get_defaults()

            # Set materialization default
            if dbt_model.materialization:
                defaults["materialization"] = dbt_model.materialization

            # Prompt for model metadata
            meta = prompt.prompt_model_metadata(dbt_model.name, defaults)

            # Prompt for column mode
            column_mode = prompt.prompt_batch_mode()

            if column_mode == "individual":
                # Prompt for each column
                console.print("\n[cyan]Collecting column metadata...[/cyan]")
                for analysis in column_analyses:
                    col_meta = prompt.prompt_column_metadata(analysis)
                    # Update analysis with user input
                    if col_meta.get("description"):
                        analysis.description_template = col_meta["description"]
                    if "data_classification" in col_meta:
                        analysis.data_classification = col_meta["data_classification"]
            # else: use smart defaults (batch or skip mode)

        # Generate YAML
        console.print("\n[cyan]Step 5:[/cyan] Generating YAML...")

        generator = YMLGenerator()

        # Determine output path
        if output:
            output_path = Path(output)
        else:
            # Default: <model_name>.yml in same directory as SQL file
            output_path = model_path_obj.with_suffix(".yml")

        # Check if file exists
        if output_path.exists() and not overwrite:
            if non_interactive:
                # Create with timestamp suffix
                from datetime import datetime
                suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = output_path.with_stem(f"{output_path.stem}_{suffix}")
                console.print(
                    f"[yellow]⚠[/yellow] File exists, creating: {output_path.name}"
                )
            else:
                # Ask user
                prompt = InteractivePrompt()
                if not prompt.confirm_overwrite(str(output_path)):
                    console.print("[yellow]Aborted by user[/yellow]")
                    return

        # Build model YAML
        model_yml = generator.generate_from_template(
            model_name=dbt_model.name,
            columns=column_analyses,
            domain=domain,
            layer=layer,
        )

        # Update with collected metadata
        model_yml["description"] = meta.get("description", model_yml["description"])
        model_yml["meta"].update(meta)

        # Write to file
        success = generator.write_yml_file(
            file_path=output_path,
            models=[model_yml],
            overwrite=overwrite,
        )

        if success:
            if non_interactive:
                console.print(f"\n[green]✓[/green] YAML file created: {output_path}\n")
            else:
                prompt = InteractivePrompt()
                prompt.display_success(str(output_path))

            console.print("[dim]Next steps:[/dim]")
            console.print(f"  1. Review and update: {output_path}")
            console.print(f"  2. Validate: miswag-dbt-yml validate {output_path}")
            console.print(f"  3. Run dbt: dbt run --select {dbt_model.name}\n")

    except Exception as e:
        console.print(f"\n[red]✗ Error:[/red] {str(e)}\n")
        import traceback
        traceback.print_exc()


@cli.command()
@click.argument("yml_path", type=click.Path(exists=True))
def validate(yml_path: str):
    """
    Validate a dbt YAML schema file

    EXAMPLE:
        miswag-dbt-yml validate models/staging/stg_op_stock.yml
    """
    console.print("\n[bold cyan]miswag-dbt-yml:[/bold cyan] YAML Validator\n")

    try:
        yml_path_obj = Path(yml_path)

        console.print(f"Validating: [yellow]{yml_path_obj}[/yellow]\n")

        validator = YMLValidator()
        is_valid, errors = validator.validate_file(yml_path_obj)

        # Display results
        validator.display_results(errors)

        if is_valid:
            console.print("[green]✓ Validation passed[/green]\n")
        else:
            console.print("[red]✗ Validation failed[/red]\n")
            exit(1)

    except Exception as e:
        console.print(f"\n[red]✗ Error:[/red] {str(e)}\n")
        exit(1)


@cli.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default="./defaults.yml",
    help="Output path for config file",
)
def init(output: str):
    """
    Create a default configuration file

    EXAMPLE:
        miswag-dbt-yml init

        miswag-dbt-yml init --output config/defaults.yml
    """
    console.print("\n[bold cyan]miswag-dbt-yml:[/bold cyan] Initialize Configuration\n")

    try:
        output_path = Path(output)

        if output_path.exists():
            console.print(f"[yellow]⚠ File exists:[/yellow] {output_path}")
            if not click.confirm("Overwrite?"):
                console.print("Aborted")
                return

        Config.create_default_config(output_path)

        console.print(f"\n[green]✓[/green] Created configuration file: {output_path}\n")
        console.print("[dim]Next steps:[/dim]")
        console.print(f"  1. Edit {output_path} with your settings")
        console.print(f"  2. Run: miswag-dbt-yml generate <model.sql>\n")

    except Exception as e:
        console.print(f"\n[red]✗ Error:[/red] {str(e)}\n")


if __name__ == "__main__":
    cli()
