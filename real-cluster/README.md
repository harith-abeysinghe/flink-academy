# Real Flink cluster (optional)

Requires Docker Desktop.

```
docker compose up -d
```

Open http://localhost:8081. There is 1 JobManager and 2 TaskManagers with 2 slots each (4 slots total).

The step-by-step exercises are in the app under **Real cluster lab**.

To stop:

```
docker compose down       # add -v to also delete checkpoints/savepoints
```
