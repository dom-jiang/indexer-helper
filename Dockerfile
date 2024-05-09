FROM python:3.10

RUN apt-get update && apt-get -y install cron

# Copy hello-cron file to the cron.d directory
COPY ./cron /etc/cron.d/cron

# Give execution rights on the cron job
RUN chmod 0777 /etc/cron.d/cron

# Apply cron job
RUN crontab /etc/cron.d/cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

WORKDIR /indexer-helper/
COPY ./ /indexer-helper/
RUN chmod -R 0777 /indexer-helper
RUN pip install -r requirements.txt

# Run the command on container startup
CMD ["cron", "-f"]