FROM python
EXPOSE 5000
WORKDIR /
COPY . .
RUN pip install psycopg2
RUN pip install flask
RUN pip install -U flask-cors
RUN pip install sqlalchemy
CMD ["python", "service_api.py"]