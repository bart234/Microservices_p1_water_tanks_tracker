FROM python:3.12-slim
WORKDIR /ServiceDBtoKafka
COPY ./requirements.txt ./       
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python","-u","service_db_to_kafka.py"]