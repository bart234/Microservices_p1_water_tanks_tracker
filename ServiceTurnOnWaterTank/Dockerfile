FROM python:3.12-slim
WORKDIR /ServiceTurnOnWaterTank
COPY ./requirements.txt ./       
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python","-u","serviceTurnOnWaterTank.py"]