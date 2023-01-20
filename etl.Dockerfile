FROM python:3.7

ADD ETL_datapipeline.py /

RUN pip install confluent-kafka\
    pip install Flask \
    pip install statsmodels \
    pip install prometheus-api-client 
    
CMD  ["python", "./ETL_datapipeline.py"]