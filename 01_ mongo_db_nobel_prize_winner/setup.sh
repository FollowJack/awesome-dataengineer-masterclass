#!/bin/bash
function virtual_env(){
  echo Create virtual environment;
  # create a virtual env venv
  virtualenv venv;
  # TODO: Find out if you really need a specific python version
  # virtualenv --python=/usr/local/bin/python3.5 venv;
  source venv/bin/activate;
  # display which python version
  which python;
  echo Install dependencies;
  pip install -r requirements.txt
}
function freeze_dependencies(){
  pip freeze > configs/requirements.txt;
}

function docker_run_compose(){
  echo Start docker compose;
  docker-compose up;
}

########################################
# Set methods public
########################################
case $1 in
  virtual_env)
    virtual_env
    ;;
  docker_run_compose)
    docker_run_compose
    ;;
  freeze_dependencies)
    freeze_dependencies
    ;;
  *)
    echo Unknown command
  ;;
esac
