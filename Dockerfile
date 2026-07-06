FROM docker.io/apache/spark:3.5.0
WORKDIR /app
USER root
COPY app.py /app/
COPY titanic.csv /app/
CMD ["/opt/spark/bin/spark-submit", "app.py"]
