FROM cloudera/quickstart:latest
ENV hostname quickstart01_gsort
RUN /usr/bin/docker-quickstart
EXPOSE 8888