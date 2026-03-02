# miswag-dbt-yml

Automated dbt YAML schema generator with data governance for ClickHouse.

## Overview

`miswag-dbt-yml` is a CLI tool that automatically generates standardized dbt YAML schema files for dbt-core models running on ClickHouse. It includes smart defaults, PII detection, and comprehensive data governance metadata based on your organization's standards.

## Features

- **ClickHouse Integration**: Connects to ClickHouse to fetch column definitions and data types
- **SQL Parsing**: Parses dbt SQL files to extract model context (config, sources, refs, columns)
- **Smart Defaults**: Automatically detects column patterns (IDs, timestamps, flags, PIPs)
- **PII Detection**: Identifies potentially sensitive data with anonymization recommendations
- **Interactive CLI**: Beautiful prompts using Rich library for easy metadata collection
- **YAML Generation**: Creates compliant YAML files using ruamel.yaml with proper formatting
- **Data Governance**: Enforces data classification, ownership, and documentation standards
- **Domain-Based Defaults**: Pre-configured defaults per business domain

## Installation

### From Source

```bash
cd /Users/miswag_hameed/Projects/dbt/miswag-dbt-yml
pip install -e .
```

### From PyPI (when published)

```bash
pip install miswag-dbt-yml
```

## Requirements

- Python 3.10+
- dbt-core 1.10.2+
- dbt-clickhouse 1.9.2+
- ClickHouse database access

## Quick Start

### 1. Configure ClickHouse Connection

Create a `defaults.yml` file in your dbt project root:

```yaml
clickhouse:
  host: localhost
  port: 8123
  username: default
  password: ""
  database: analytics_db

defaults:
  tech_owner: hameed_mahmood
  business_owner: data_team
```

### 2. Generate YAML for a Model

```bash
# Basic usage - will prompt for required fields
miswag-dbt-yml generate models/staging/stg_op_stock.sql

# Skip ClickHouse connection
miswag-dbt-yml generate models/staging/stg_op_stock.sql --skip-clickhouse

# Non-interactive mode (uses defaults, comments required fields)
miswag-dbt-yml generate models/staging/stg_op_stock.sql --non-interactive
```

### 3. Validate Existing YAML

```bash
miswag-dbt-yml validate models/staging/stg_op_stock.yml
```

## Architecture

### Core Modules

1. **constants.py**: Governance rules, allowed values, PII patterns
2. **clickhouse_client.py**: ClickHouse connection and schema introspection
3. **sql_parser.py**: dbt SQL file parsing (config, sources, refs, columns)
4. **column_analyzer.py**: Smart defaults and PII detection
5. **yml_generator.py**: YAML generation using ruamel.yaml
6. **interactive.py**: Rich-based CLI prompts
7. **validator.py**: YAML validation against standards
8. **config.py**: Configuration management
9. **cli.py**: Click commands and CLI interface

## Smart Defaults

### Column Pattern Detection

The tool automatically detects column types based on naming patterns:

- **Primary Keys**: `*_id`, `id`, `*_pk`
- **Foreign Keys**: `*_fk`, `*_ref`
- **Timestamps**: `*_at`, `*_date`, `*_time`, `created`, `updated`
- **Flags**: `is_*`, `has_*`, `can_*`, `*_flag`
- **Counts**: `*_count`, `*_cnt`, `total_*`
- **Amounts**: `*_amount`, `*_value`, `*_price`

### PII Detection

Automatically detects PII with anonymization recommendations:

- **Phone Numbers**: `phone`, `mobile` → hashed
- **Email**: `email` → masked
- **Names**: `first_name`, `last_name` → tokenized
- **Date of Birth**: `dob`, `date_of_birth` → masked (convert to age)
- **National IDs**: `ssn`, `national_id`, `passport` → encrypted
- **Addresses**: `address`, `street`, `city` → masked
- **Credit Card**: `credit_card`, `card_number` → encrypted

## Data Governance Standards

### Required Model Metadata

- `tech_owner`: Technical owner (person responsible for model)
- `business_owner`: Business owner (person/team who owns the data)
- `data_classification`: Public, Internal, PII, Sensitive, Restricted
- `source_system`: Source system name (e.g., miswagdb, rudderstack)
- `materialization`: view, table, incremental, ephemeral
- `pii`: Boolean flag if model contains PII
- `domain`: Business domain (operations, search, customer, etc.)

### Optional Model Metadata

- `data_loading`: How data is loaded (mageai, rudderstack, etc.)
- `source_frequency`: How often source data updates
- `update_frequency`: How often model updates
- `sla_hours`: SLA in hours for model freshness
- `data_steward`: Data steward name
- `data_producer`: System/team producing the data
- `data_consumers`: List of downstream consumers

### Column Metadata

- `data_classification`: Classification level
- `pii`: Boolean flag if column contains PII
- `anonymization_method`: none, hashed, masked, tokenized, encrypted
- `business_rules`: Business rules governing the column
- `calculation_logic`: How calculated/derived columns are computed

## Domain-Based Defaults

Pre-configured defaults per domain:

### Operations Domain
- `business_owner`: suhib_falih
- `source_system`: miswagdb
- `data_loading`: mageai

### Search Domain
- `business_owner`: data_team
- `source_system`: rudderstack
- `data_loading`: rudderstack

### Customer Domain
- `business_owner`: customer_success
- `source_system`: miswagdb
- `data_loading`: mageai

## Configuration File

Create `defaults.yml` in your dbt project root:

```yaml
# ClickHouse connection
clickhouse:
  host: localhost
  port: 8123
  username: default
  password: ""
  database: analytics_db

# Default values
defaults:
  tech_owner: hameed_mahmood
  business_owner: data_team
  data_classification: internal
  source_system: miswagdb
  data_loading: mageai

# Domain-specific defaults
domains:
  operations:
    business_owner: suhib_falih
    source_system: miswagdb

  search:
    business_owner: data_team
    source_system: rudderstack
```

## Examples

### Example 1: Generate YAML for Staging Model

```bash
miswag-dbt-yml generate models/staging/mysql_replication/stg_op_stock.sql
```

Output:
```yaml
version: 2

models:
  - name: stg_op_stock
    description: "Stock inventory staging model"
    tags: ["staging", "operations"]
    config:
      contract:
        enforced: false  # Always false for ClickHouse - tests are the only enforcement layer
    meta:
      tech_owner: hameed_mahmood
      business_owner: suhib_falih
      data_classification: internal
      source_system: miswagdb
      data_loading: mageai
      materialization: view
      pii: false
      domain: operations

    columns:
      - name: item_id
        description: "Unique identifier for item ID"
        data_type: UInt64
        tests:
          - not_null
        meta:
          data_classification: internal
          pii: false

      - name: st_qty
        description: "Stock quantity"
        data_type: Int32
        meta:
          data_classification: internal
          pii: false
```

### Example 2: Non-Interactive Mode

```bash
miswag-dbt-yml generate models/search/marts/mart_search_queries.sql --non-interactive
```

Creates YAML with commented required fields for manual filling:

```yaml
version: 2

models:
  - name: mart_search_queries
    description: "Mart Search Queries"  # TODO: Add detailed description
    meta:
      tech_owner: ""  # TODO: Add technical owner
      business_owner: data_team
      data_classification: internal
```

## Development Status

**Current Version**: 0.1.0 (Alpha)

**Completed**:
- ✓ Project structure and setup
- ✓ Constants and governance rules
- ✓ ClickHouse client with schema introspection
- ✓ SQL parser for dbt models
- ✓ Column analyzer with smart defaults and PII detection
- ✓ YAML generator using ruamel.yaml

**In Progress**:
- Interactive CLI prompts
- Validator module
- Configuration management
- Main CLI commands

**Planned**:
- Batch processing for multiple models
- Template customization
- Integration tests
- Documentation site

## Contributing

This is an internal tool for Miswag. Contact Hameed Mahmood for questions or contributions.

## License

MIT License

## Credits

Developed by Hameed Mahmood for Miswag's data governance initiative.
