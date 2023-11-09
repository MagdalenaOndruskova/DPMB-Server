FROM python:3.11

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

##
#CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--reload", "--port", "80"]
