# visualizing-anomalies
Visualizing anomalies with Bytewax and Rerun

### Step 1:
Run redpanda via the docker compose yaml file

```sh
docker compose up -d
```

### Step 2:
run the helpers.py script which will start populating the "sensors" kafka topic.

```sh
python helpers.py
```

### Step 3:
Open redpanda console at the link - http://localhost:8080/

### Step 4:
Run rerun as a separate process

```sh
python -m rerun
```

Step 5:

Run the dataflow and watch for anomalies in the visualization. You can scale it to many processes.

```python
python -m bytewax.run dataflow:flow -p 3
```
