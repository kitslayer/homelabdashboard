FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends sshpass openssh-client && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py index.html history.html console.html ./
COPY photos ./photos
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--loop", "uvloop", "--backlog", "2048"]
