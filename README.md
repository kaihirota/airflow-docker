## Airflow

Port: 8080

Access: localhost:8080

Explanation:
<!-- TODO: explain dockerfile and entrypoint -->

```bash
# inside airflow_docker/
docker build --rm -t airflow_docker:latest .
docker run --rm -p 8080:8080 --env-file ../.env --name airflow airflow_docker:latest

docker exec -it airflow airflow trigger_dag 'dag_name' -r 'run_id' --conf '{"ticker": "BYND", "limit": 100}'

# Trigger DAG from command line using Airflow CLI
docker exec -it airflow airflow trigger_dag 'Reddit_Sentiment_Analysis' -r 'initial_test' --conf '{"ticker": "BYND", "limit": 100}'
# Reddit_Sentiment_Analysis = dag_name
# initial_test = run_id
# ticker: enter the ticker symbol of the stock
# limit: 1 - 1000, how many Reddit posts to download


docker exec -it airflow airflow trigger_dag 'Monitor_Watchlist' -r 'initial_test'
```
