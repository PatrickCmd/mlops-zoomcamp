FROM agrigorev/zoomcamp-model:mlops-3.9.7-slim

# set output directory
ENV OUTPUT=output/fhv_tripdata_result

RUN pip install -U pip
RUN pip install pipenv

WORKDIR /app

RUN mkdir -p $OUTPUT

COPY [ "Pipfile", "Pipfile.lock", "./" ]

RUN pipenv install --system --deploy

COPY ["starter.py", "./"]
