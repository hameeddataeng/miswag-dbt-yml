"""
miswag-dbt-yml: Automated dbt YAML schema generator with data governance
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="miswag-dbt-yml",
    version="0.1.0",
    author="Hameed Mahmood",
    author_email="hameed@miswag.com",
    description="Automated dbt YAML schema generator with data governance for ClickHouse",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hameeddataeng/miswag-dbt-yml",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Code Generators",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.10",
    install_requires=[
        "click>=8.1.0",
        "clickhouse-connect>=0.6.0",
        "ruamel.yaml>=0.17.0",
        "sqlparse>=0.4.0",
        "rich>=13.0.0",
        "pydantic>=2.0.0",
    ],
    entry_points={
        "console_scripts": [
            "miswag-dbt-yml=dbt_yml_gen.cli:cli",
        ],
    },
    include_package_data=True,
    package_data={
        "dbt_yml_gen": ["config/*.yml", "templates/*.yml"],
    },
)
