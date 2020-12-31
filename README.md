# FRANCE_JOBS_DATASET_GENERATOR
This project retrieve jobs titles from Pole Emploi (French unemployment agency) Open Data then scraps Indeed to create a dataset that counts the number of jobs with a track record during the last 30 days and per french metropolitan department.

## Installation
Prerequisites
- Python version 3.8+
- pip version 20+

### Environment
Option 1 : 
create a python virtual environment to run this project with the following command
<br />``python3 -m venv /path/to/new/virtual/environment``<br />
Then activate it with following command 
<br />``source env/bin/activate``<br />
Then install the dependencies with following command
<br />``pip install -f requirements.txt``<br />

Option 2 : create a conda environment (in that case conda is a prerequisite)
<br />``conda create --name <env> --file requirements.txt``<br />
Then activate it with following command 
<br />``conda activate <env>``<br />






