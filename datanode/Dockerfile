FROM python:3.11-slim
WORKDIR /app
COPY datanode.py .
RUN mkdir -p data_blocks
EXPOSE 5001
CMD ["python3", "datanode.py"]