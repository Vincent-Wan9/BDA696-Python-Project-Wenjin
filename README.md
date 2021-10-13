# PythonProject

# Setup for developement:

- Setup a python 3.x venv (usually in `.venv`)
  - You can run `./scripts/create-venv.sh` to generate one
- `pip3 install --upgrade pip`
- Install pip-tools `pip3 install pip-tools`
- Update dev requirements: `pip-compile --output-file=requirements.dev.txt requirements.dev.in`
- Update requirements: `pip-compile --output-file=requirements.txt requirements.in`
- Install dev requirements `pip3 install -r requirements.dev.txt`
- Install requirements `pip3 install -r requirements.txt`
- `pre-commit install`

## Update versions

`pip-compile --output-file=requirements.dev.txt requirements.dev.in --upgrade`

# Run `pre-commit` locally.

`pre-commit run --all-files`

# Steps for Assignment 3

1. Add "pyspark" and "pyspark-stubs" library in "requirements.in". And then update and install requirements.
2. Before running the code, make sure you have JDBC connector installed and saved in .venv/lib/python3.8/site-packages/pyspark/jars
   - JDBC connector can be downloaded from https://dev.mysql.com/downloads/connector/j/5.1.html
   - On the webpage, select "Platform Independent" as your operating system and then download the first package.
   - Once downloaded, copy "mysql-connector-java-8.0.26.jar" into .venv/lib/python3.8/site-packages/pyspark/jars
3. After setting up JDBC, type `python3 Assignment3.py` to run the script.
