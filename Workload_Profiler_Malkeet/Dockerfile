FROM python
EXPOSE 5000
COPY . .
RUN pip install psycopg2
RUN pip install flask
RUN pip install flask-cors
CMD ["python", "service_api.py"]