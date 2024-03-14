FROM python:3.11

WORKDIR /app

COPY . /app


RUN apt update && apt install cmake cmake-doc ninja-build lrzip libblas3 liblapack3 liblapack-dev libblas-dev libatlas-base-dev libgdal-dev gdal-bin qgis -y
RUN apt install gdal-bin

RUN pip install -r requirements.txt

CMD [ "uvicorn", "main:app", "--port=8000", "--host=0.0.0.0"]
