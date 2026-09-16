FROM python:3.12-slim
WORKDIR /ServiceSMS
COPY ./requirements.txt ./       
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python","-u","service_sms.py"]