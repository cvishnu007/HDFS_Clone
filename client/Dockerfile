FROM python:3.11-slim
WORKDIR /app
COPY client.py .
RUN pip install --no-cache-dir streamlit
EXPOSE 8501
CMD ["streamlit", "run", "client.py", "--server.address=0.0.0.0"]