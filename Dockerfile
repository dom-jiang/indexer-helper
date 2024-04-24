FROM python:3.10

RUN apt-get update && apt-get -y install cron

# Copy hello-cron file to the cron.d directory
WORKDIR /indexer-helper/
COPY ./ /indexer-helper/
COPY ./cron /etc/cron.d/

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/cron

# Apply cron job
RUN crontab /etc/cron.d/cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

RUN pip install -r requirement.txt &&

# Run the command on container startup
CMD ["cron", "-f"]