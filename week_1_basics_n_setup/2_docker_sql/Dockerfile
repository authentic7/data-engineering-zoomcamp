FROM python:3.9.1

WORKDIR /app


COPY requirements.txt requirements.txt
COPY ingest_data.py ingest_data.py 

RUN apt-get install wget
RUN pip3 install -r requirements.txt
ENTRYPOINT [ "python", "ingest_data.py" ]