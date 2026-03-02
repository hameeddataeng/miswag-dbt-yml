# Quick Start Guide

## Installation

```bash
cd /Users/miswag_hameed/Projects/dbt/miswag-dbt-yml
pip install -e .
```

This installs the `miswag-dbt-yml` command globally.

## Verify Installation

```bash
miswag-dbt-yml --version
# Should output: miswag-dbt-yml, version 0.1.0

miswag-dbt-yml --help
```

## Step 1: Initialize Configuration

Navigate to your dbt project root:

```bash
cd /Users/miswag_hameed/Projects/dbt/rudder_analytics

# Create default configuration
miswag-dbt-yml init
```

This creates `defaults.yml` in your current directory. Edit it with your settings:

```yaml
clickhouse:
  host: your-clickhouse-host
  port: 8123
  username: default
  password: your-password
  database: analytics_db

defaults:
  tech_owner: hameed_mahmood
  business_owner: data_team
```

## Step 2: Generate YAML for a Model

### Example 1: Interactive Mode (Recommended for First Time)

```bash
miswag-dbt-yml generate models/operations/staging/mysql_replication/stg_op_stock.sql
```

This will:
1. Parse the SQL file
2. Connect to ClickHouse to get column types
3. Detect PII automatically
4. Prompt you for metadata
5. Generate the YAML file

### Example 2: Non-Interactive Mode (Fast)

```bash
miswag-dbt-yml generate models/operations/marts/mart_stock.sql --non-interactive
```

Uses smart defaults, perfect for batch processing.

### Example 3: Skip ClickHouse Connection

```bash
miswag-dbt-yml generate models/search/marts/mart_search_queries.sql --skip-clickhouse
```

Useful when ClickHouse is not available or table doesn't exist yet.

## Step 3: Validate Generated YAML

```bash
miswag-dbt-yml validate models/operations/marts/mart_stock.yml
```

This checks for:
- Missing required fields
- Invalid values
- PII without anonymization
- Compliance with governance standards

## Common Use Cases

### Generate YAML for Operations Model

```bash
cd /Users/miswag_hameed/Projects/dbt/rudder_analytics

miswag-dbt-yml generate models/operations/marts/mart_stock.sql
```

### Generate YAML for Search Model

```bash
miswag-dbt-yml generate models/search/marts/mart_search_queries.sql
```

### Batch Generate (Multiple Models)

```bash
# Generate for all staging models in operations
for file in models/operations/staging/**/*.sql; do
    miswag-dbt-yml generate "$file" --non-interactive
done
```

### Validate All YML Files

```bash
# Validate all yml files in models directory
find models -name "*.yml" -type f | while read file; do
    echo "Validating $file"
    miswag-dbt-yml validate "$file"
done
```

## Tips

1. **Start Interactive**: First time using the tool, run in interactive mode to understand the prompts
2. **Use Non-Interactive for Batch**: When generating many files, use `--non-interactive`
3. **Review Generated Files**: Always review generated YAML and update descriptions
4. **Validate Before Commit**: Run validator before committing to ensure compliance

## CLI Options Summary

### `miswag-dbt-yml generate`

```
miswag-dbt-yml generate [OPTIONS] MODEL_PATH

Options:
  -o, --output PATH          Output YAML file path
  --skip-clickhouse         Skip ClickHouse connection
  --non-interactive         Non-interactive mode with defaults
  --config PATH             Path to config file
  --overwrite               Overwrite existing YAML
  --help                    Show help
```

### `miswag-dbt-yml validate`

```
miswag-dbt-yml validate YML_PATH

Validates YAML against governance standards
```

### `miswag-dbt-yml init`

```
miswag-dbt-yml init [OPTIONS]

Options:
  -o, --output PATH    Output path for config file (default: ./defaults.yml)
  --help              Show help
```

## Example Workflow

```bash
# 1. Navigate to dbt project
cd /Users/miswag_hameed/Projects/dbt/rudder_analytics

# 2. Initialize config (first time only)
miswag-dbt-yml init

# 3. Edit config with your settings
vi defaults.yml

# 4. Generate YAML for a model
miswag-dbt-yml generate models/operations/marts/mart_stock.sql

# 5. Review and edit the generated YAML
vi models/operations/marts/mart_stock.yml

# 6. Validate the YAML
miswag-dbt-yml validate models/operations/marts/mart_stock.yml

# 7. Run dbt
dbt run --select mart_stock

# 8. Commit changes
git add models/operations/marts/mart_stock.yml
git commit -m "Add schema for mart_stock"
```

## Troubleshooting

### "Command not found: miswag-dbt-yml"

Re-install in editable mode:
```bash
cd /Users/miswag_hameed/Projects/dbt/miswag-dbt-yml
pip install -e .
```

### "Could not connect to ClickHouse"

Either:
- Fix ClickHouse connection in `defaults.yml`
- Use `--skip-clickhouse` flag

### "No columns found"

The SQL parser couldn't extract columns. This can happen with complex SQL. Use `--skip-clickhouse` to fall back to simple parsing, or manually edit the generated YAML.

### PII Warnings

Review each PII detection carefully. The tool uses pattern matching and may have false positives. Update the `pii` flag and `anonymization_method` as needed.

## Next Steps

1. Generate YAML for your existing models
2. Validate all YAML files
3. Update dbt project documentation
4. Set up pre-commit hooks to validate on commit
5. Train team on using the tool

## Support

Contact: Hameed Mahmood (hameed@miswag.com)
