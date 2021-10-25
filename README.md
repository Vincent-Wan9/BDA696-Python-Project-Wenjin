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

# Steps for Assignment 4

1. Add "statsmodels" in "requirements.in". And then update and install requirements.
2. This python code assumes the response is continuous and the loaded dataset is clean, including
   - removing unwanted column,
   - drop na value
   - correct data type,
   - no multi-categorical response variable
3. plots are saved in .../Homework/Data
4. for testing, when running the code, enter `/Users/Vincentwang/PycharmProjects/BDA696-Python-Project-Wenjin/Homework/Data/Telco-Customer-Churn.csv` as your dataset location. And then type `tenure` as response
