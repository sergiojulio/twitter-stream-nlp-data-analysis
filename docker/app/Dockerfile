FROM python:3.10-slim
WORKDIR /code
COPY src/app/requirements.txt .
COPY src/ .
RUN ls -la /*
RUN pip install --no-cache-dir -r /code/requirements.txt
CMD ["uvicorn", "src.app.fastapi.main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"] 