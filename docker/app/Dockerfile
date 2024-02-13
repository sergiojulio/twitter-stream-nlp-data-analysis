FROM python:3.10-slim
WORKDIR /code
COPY ./src/app/requirements.txt ./
RUN pip install --no-cache-dir -r /code/requirements.txt
COPY ./src /code
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"] 