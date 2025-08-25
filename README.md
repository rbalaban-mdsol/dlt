# DLT Example

Example of how to use DLT for ingestion into Iceberg ( via Glue catalog )

## Setup

This repo exclusivley uses UV. 

Install Python UV, find the docs on how to install UV [here](https://docs.astral.sh/uv/getting-started/installation/#installation-methods).

1. Run: `uv sync`
2. Run commands with: `uv run python ...`


### Resources

1. [https://dlthub.com/](Official Website)
2. [https://dlthub.com/community](Slack Channel)


### How it works.

1. Replace all .env credentials with the correct ones.
2. Run `setup.py` - this creates the required database and table if it does not exist
3. Run `main.py` - this will load the result .parquet file into Iceberg. 
