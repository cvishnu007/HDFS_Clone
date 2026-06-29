FROM python:3.11-slim
WORKDIR /app
COPY namenode.py .
RUN pip install --no-cache-dir prometheus_client && \
    mkdir -p logs data
EXPOSE 5000
EXPOSE 6000/udp
EXPOSE 8080
CMD ["python3", "namenode.py"]