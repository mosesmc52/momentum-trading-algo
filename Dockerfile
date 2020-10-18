FROM python:2.7.17


# to the terminal with out buffering it first
ENV PYTHONUNBUFFERED 1

# prevents python from writing pyc files to disk
ENV PYTHONDONTWRITEBYTECODE 1

WORKDIR /app
COPY ./requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt
RUN rm requirements.txt

COPY . /app

# Install cron
RUN apt-get update
RUN apt-get install -y cron
RUN apt-get install nano

# Add files
ADD docker_entrypoint.sh /docker_entrypoint.sh

RUN touch /var/log/cron.log

ENTRYPOINT /docker_entrypoint.sh && tail -f /var/log/cron.log
