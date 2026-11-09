FROM python:3.12-slim
WORKDIR /app
COPY ./requirements.txt ./       
RUN pip install --no-cache-dir -r requirements.txt
COPY . ./ServiceDBtoKafka
CMD ["python","-u","-m","ServiceDBtoKafka.service_main"]