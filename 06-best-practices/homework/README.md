# Home Work

Run batch file

```
python batch.py --year 2021 --month 2
```

Run unit tests

```
pipenv run pytest -v
```

Setup LocalStack

```
docker-compose up -d
```

Create bucket with aws localstack
```
aws --endpoint-url http://localhost:4566 s3 mb s3://nyc-duration
```

List aws buckets on localstack

```
aws --endpoint-url http://localhost:4566 s3 ls
```

List objects in aws bucket on localstack
```
aws --endpoint-url http://localhost:4566 s3 ls s3://nyc-duration
```

Export S3_ENDPOINT_URL

```
export S3_ENDPOINT_URL="http://localhost:4566/nyc-duration"
```

Run integration test

```
python integration_test.py --year 2021 --month 1
```