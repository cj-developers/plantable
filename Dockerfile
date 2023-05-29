FROM python:3.9-slim-bullseye

# SETTINGS
WORKDIR /workdir 

# Update and Upgrade Libraries
RUN apt-get update && apt-get install -y curl
RUN python -m pip install --upgrade pip

# Copy and Install Package
COPY . ./
RUN python -m pip install .
