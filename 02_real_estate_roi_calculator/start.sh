#!/bin/bash
echo Create virtual env
virtualenv venv02_real;
source venv02_real/bin/activate;
echo Install kernel for environment
pip install ipykernel;
python3 -m ipykernel install --user;
echo Install dependencies
pip install -r requirements.txt
