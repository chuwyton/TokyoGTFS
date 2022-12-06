# syntax=docker/dockerfile:1

# create the base image with the dependencies
FROM python:3.7-alpine AS baseimage
WORKDIR /install/
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# Now add in the app source code
FROM baseimage AS build
WORKDIR /app/
COPY /data ./data
COPY /src ./src
COPY *.py .
ENTRYPOINT ["python3"]
