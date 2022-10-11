# Delta lake demo

This project use Poetry to manage dependencies

You can install Poetry by following the instructions [here](https://python-poetry.org/docs/#installing-with-the-official-installer)


I recommend the pipx route, but your mileage may vary

Then, simply run `poetry install` to install the dependencies or `make install`

Then, to start the notebook run `poetry run jupyter-lab` or `make notebook`

## Ingestion
Start the ingestion job with `poetry run python delta-demo/ingest.py` or `make ingest`