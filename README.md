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

# Steps for HW5

1. update and install requirement (mariadb & sqlalchemy)
2. run "Assignment5.sql" to get the data (using "source Homework/Assignment5.sql")
   1. Note: go to "baseball" database on Mariadb before running the sql code
   2. It takes time to generate the data (for me, I've to wait 10 mins)
3. run "Assignment5.py"
