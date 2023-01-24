FROM python:3.7

ADD datastorage.py /

RUN python -m pip install --upgrade pip \ 
    pip install mysql-connector-python\
    pip install confluent-kafka

CMD  ["python", "./datastorage.py" ]