"""
Constants and governance rules for miswag-dbt-yml
Based on dbt_yml_standard_reference.yml
"""

# Data classification levels
DATA_CLASSIFICATIONS = [
    "Public",
    "Internal",
    "PII",
    "Sensitive",
    "Restricted",
]

# Anonymization methods
ANONYMIZATION_METHODS = [
    "none",
    "hashed",
    "masked",
    "tokenized",
    "encrypted",
]

# Model layers and prefixes
MODEL_LAYERS = {
    "staging": {"prefix": "stg_", "materialization": "view"},
    "intermediate": {"prefix": "int_", "materialization": "view"},
    "mart": {"prefix": "mart_", "materialization": "table"},
    "dimension": {"prefix": "dim_", "materialization": "table"},
    "fact": {"prefix": "fact_", "materialization": "table"},
}

# Domains
DOMAINS = [
    "operations",
    "search",
    "customer",
    "product",
    "marketing",
    "finance",
    "logistics",
]

# Materialization types
MATERIALIZATION_TYPES = [
    "view",
    "table",
    "ephemeral",
    "incremental",
]

# PII handling rules (column_pattern -> anonymization_method)
PII_PATTERNS = {
    # Phone numbers
    r"phone|mobile|tel": "hashed",
    r".*_phone$|.*_mobile$|.*_tel$": "hashed",

    # Email addresses
    r"email|e_mail": "masked",
    r".*_email$": "masked",

    # Names
    r"first_name|last_name|full_name|customer_name|user_name": "tokenized",
    r".*_name$": "tokenized",

    # Date of birth
    r"dob|date_of_birth|birth_date": "masked",

    # National IDs
    r"ssn|national_id|passport|driver_license": "encrypted",

    # Addresses
    r"address|street|city|postal_code|zip_code": "masked",

    # IP addresses
    r"ip_address|ipv4|ipv6": "hashed",

    # Credit card
    r"credit_card|card_number|cvv": "encrypted",

    # Bank account
    r"bank_account|iban|swift": "encrypted",
}

# Column name patterns for smart defaults
COLUMN_PATTERNS = {
    # Primary keys
    "is_primary_key": [
        r".*_id$",
        r"^id$",
        r".*_pk$",
    ],

    # Foreign keys
    "is_foreign_key": [
        r".*_fk$",
        r".*_ref$",
    ],

    # Timestamps
    "is_timestamp": [
        r".*_at$",
        r".*_date$",
        r".*_time$",
        r".*_ts$",
        r"created|updated|deleted",
    ],

    # Flags/Booleans
    "is_flag": [
        r"^is_.*",
        r"^has_.*",
        r"^can_.*",
        r".*_flag$",
        r".*_ind$",
    ],

    # Counts
    "is_count": [
        r".*_count$",
        r".*_cnt$",
        r"total_.*",
        r"num_.*",
    ],

    # Amounts/Values
    "is_amount": [
        r".*_amount$",
        r".*_value$",
        r".*_price$",
        r".*_cost$",
    ],
}

# ClickHouse data type mappings
CLICKHOUSE_TYPES = {
    # Integer types
    "UInt8": "UInt8",
    "UInt16": "UInt16",
    "UInt32": "UInt32",
    "UInt64": "UInt64",
    "Int8": "Int8",
    "Int16": "Int16",
    "Int32": "Int32",
    "Int64": "Int64",

    # Float types
    "Float32": "Float32",
    "Float64": "Float64",
    "Decimal": "Decimal",

    # String types
    "String": "String",
    "FixedString": "FixedString",

    # Date/Time types
    "Date": "Date",
    "Date32": "Date32",
    "DateTime": "DateTime",
    "DateTime64": "DateTime64",

    # Boolean
    "Bool": "Bool",

    # Array
    "Array": "Array",

    # Nullable wrapper
    "Nullable": "Nullable",
}

# dbt test types
DBT_TEST_TYPES = [
    "not_null",
    "unique",
    "accepted_values",
    "relationships",
]

# Required metadata fields at model level
REQUIRED_MODEL_META = [
    "tech_owner",
    "business_owner",
    "data_classification",
    "source_system",
    "materialization",
    "pii",
    "domain",
]

# Optional metadata fields at model level
OPTIONAL_MODEL_META = [
    "data_loading",
    "source_frequency",
    "update_frequency",
    "sla_hours",
    "data_steward",
    "data_producer",
    "data_consumers",
]

# Required metadata fields at column level
REQUIRED_COLUMN_META = [
    "data_classification",
    "pii",
]

# Optional metadata fields at column level
OPTIONAL_COLUMN_META = [
    "anonymization_method",
    "business_rules",
    "calculation_logic",
    "data_steward",
]

# Composite key generation rules
COMPOSITE_KEY_RULES = {
    "max_columns": 5,  # Maximum columns in composite key
    "separator": "_",  # Separator for composite key naming
}

# YAML formatting preferences
YAML_FORMATTING = {
    "indent": 2,
    "block_seq_indent": 0,  # Offset from mapping indent for list item content
    "line_width": 120,
    "preserve_quotes": False,
}

# Validation rules
VALIDATION_RULES = {
    "max_description_length": 500,
    "min_description_length": 10,
    "required_tests_for_pk": ["not_null", "unique"],
    "required_tests_for_fk": ["not_null", "relationships"],
}

# Default values for different domains
DOMAIN_DEFAULTS = {
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
}

# Contract enforcement (always false for ClickHouse)
CONTRACT_ENFORCED = False

# Version
DBT_SCHEMA_VERSION = 2
