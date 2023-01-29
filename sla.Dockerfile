FROM python:3.7

ADD SLA_manager.py /

RUN pip install Flask\
    pip install statsmodels\
    pip install prometheus-api-client

CMD  ["python", "./SLA_manager.py" ]
