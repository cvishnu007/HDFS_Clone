FROM python:3.11-slim
WORKDIR /app
COPY namenode.py .
RUN mkdir -p logs data
EXPOSE 5000
EXPOSE 6000/udp
CMD ["python3", "namenode.py"]