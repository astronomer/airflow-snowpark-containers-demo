# syntax=quay.io/astronomer/airflow-extensions:latest

FROM quay.io/astronomer/astro-runtime:8.6.0-base

COPY include/astro_provider_snowflake-0.0.0-py3-none-any.whl /tmp

#Installing dbt in venv due to pyarrow dependency issues.
#https://github.com/astronomer/astro-sdk/blob/08b73675bd6855e11bc9bcbfd089a0ba4536c52d/python-sdk/pyproject.toml#L67
#https://github.com/dbt-labs/dbt-snowflake/blob/003d8946e10c9aafdf1517b645a73652a0fabe0c/setup.py#L71
PYENV 3.9 dbt requirements-dbt.txt