FROM python:3.7.4

COPY requirements.txt requirements.txt

Add . .

RUN apt-get update \
    && pip install -r requirements.txt \
    && cd plaque_assay \
    && python setup.py install \
    && cd ..

COPY . .
