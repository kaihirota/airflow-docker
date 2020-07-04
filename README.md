# reddit-stock-sentiment-tracker

## Usage (in progress)

1. Create `.env` file inside docker_airflow. This is where you will store secret keys.

TODO: Add template `.env` file

## Docker-compose

```bash
# Build both containers, and compose / run in one command
docker-compose up --build

# First two commands build the containers. Once built, just run the last command to compose / run to save time.
# in airflow_docker/
docker build --rm -f Dockerfile -t airflow_image:latest .
# in superset_docker/
docker build --rm -f airflow_superset/Dockerfile -t superset_image:latest .
docker-compose up
```

## Airflow

Port: 8080
Access: localhost:8080

Explanation:
<!-- TODO: explain dockerfile and entrypoint -->

```bash
# inside airflow_docker/
docker build --rm -f Dockerfile -t airflow_image:latest .
docker run --rm -p 8080:8080 --env-file ../.env --name reddit_nlp airflow_image:latest

docker exec -it reddit_nlp airflow trigger_dag 'dag_name' -r 'run_id' --conf '{"ticker": "BYND", "limit": 100}'

# Trigger DAG from command line using Airflow CLI
docker exec -it reddit_nlp airflow trigger_dag 'Reddit_Sentiment_Analysis' -r 'initial_test' --conf '{"ticker": "BYND", "limit": 100}'
# Reddit_Sentiment_Analysis = dag_name
# initial_test = run_id
# ticker: enter the ticker symbol of the stock
# limit: 1 - 1000, how many Reddit posts to download
```

## Superset

Port: 9999
Access: localhost:9999

```bash
# inside superset_docker/
docker build --rm -f Dockerfile -t superset_image:latest .
docker run --rm -d -p 9999:8088 --env-file ../.env --name superset superset_image:latest
```
