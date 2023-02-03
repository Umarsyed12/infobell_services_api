FROM python
EXPOSE 5000

# RUN adduser root root

RUN mkdir uploaded_files \
 && chown -R :root /uploaded_files \
 && chmod -R 0775 /uploaded_files

# WORKDIR /
COPY . .
WORKDIR /

# RUN chgrp -R 0 /root && \
#     chmod -R g=u //root

# RUN chmod -R 755 /root
# RUN chown -R root:root /root

# COPY . .

RUN pip install psycopg2
RUN pip install flask
RUN pip install flask-cors
RUN pip install sqlalchemy
RUN pip install pandas

CMD ["python", "service_api.py"]




# FROM node:7.4-alpine

# RUN npm install --global yarn pm2

# RUN adduser node root
# COPY . /home/node/app
# WORKDIR /home/node/app
# RUN chmod -R 755 /home/node/app
# RUN chown -R node:node /home/node/app

# RUN yarn install --production

# EXPOSE 8100

# USER 1000

# CMD ["pm2-docker", "start", "--auto-exit", "--env", "production", "process.yml"]