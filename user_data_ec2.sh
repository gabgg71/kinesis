#!/bin/bash
sudo apt update
sudo apt upgrade
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.8
sudo apt install python3-pip
pip install boto3
pip install python-dotenv
pip install numpy

