# Real Estate Roi Calculator

This is an ETL pipeline for real estate data.
Aim of the project is to crawl over an API and store them in a postgresql database.

## Setup

In short run `start.sh` for all of the steps below.

Or manually:
Start a virtual env

```
virtualenv venv02_real;
source venv02_real/bin/activate;
pip install ipykernel;
python3 -m ipykernel install --user;
```

Install requirements

```
pip install -r requirements.txt
```

Run ETL pipeline or jupyter notebook

## Run

In order to run ETL pipeline execute `python pipeline`

## Architecture

/pipeline/ETL as ETL entry for the pipeline
/notebooks/playfield used as playfield to try out different approaches
