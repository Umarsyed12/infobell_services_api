FROM python
EXPOSE 5000
WORKDIR /
COPY . .
RUN pip install psycopg2
RUN pip install flask
RUN pip install flask-cors
RUN pip install sqlalchemy
RUN pip install pandas
CMD ["python", "service_api.py"]