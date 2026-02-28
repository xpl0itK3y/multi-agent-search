FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8:replace
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "-u", "main.py"]
