FROM python:3.7

ADD dataretrieval.py /

RUN pip install Flask\
    pip install mysql-connector-python

CMD  ["python", "./dataretrieval.py" ]