import json
import rerun as rr

from datetime import datetime

from bytewax.dataflow import Dataflow
from bytewax.connectors.kafka import KafkaInput
from bytewax.connectors.stdio import StdOutput

from river import anomaly, preprocessing

rr.init("warehouse sensors")
rr.connect(addr="0.0.0.0:9876")

flow = Dataflow()
flow.input("input", KafkaInput(["localhost:19092"], ["sensors"]))
# ("sensor_id", "{"ts":"2023..."}")

# flow.inspect(print)

def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes
    sensor_id = str(json.loads(key_bytes.decode())) if key_bytes else None
    data = json.loads(payload_bytes.decode()) if payload_bytes else None
    return sensor_id, data

flow.map(deserialize)

# We need to normalize our data for the anomaly detection algorithm.
# Since we know the min and max in our contrived example,
# we can scale using the min_max_scalar.
def min_max_scalar(sensor_id__data: tuple, min: float, max: float):
    """
    Our anomaly detector requires normalized data between 0 and 1.
    This function will scale our data between the min and max.

    min (float) - lowest value in the dataset
    max (float) - max value in the dataset
    """
    sensor_id, data = sensor_id__data
    data['temp_normalized'] = (data['temp']-min)/(max-min)
    return (sensor_id, data)

flow.map(lambda x: min_max_scalar(x, 21.0, 85.0))

class AnomalyDetector(anomaly.HalfSpaceTrees):
    """
    Our anomaly detector inherits from the HalfSpaceTrees
    object from the river package and has the following inputs


    n_trees – defaults to 10
    height – defaults to 8
    window_size – defaults to 250
    limits (Dict[Hashable, Tuple[float, float]]) – defaults to None
    seed (int) – defaults to None

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, n_trees=5, height=3, window_size=30, seed=42, **kwargs)

    def update(self, data):
        print(data)
        dt = datetime.strptime(data["ts"], '%Y-%m-%d %H:%M:%S')
        t = int(dt.timestamp())
        data["score"] = self.score_one({"value": data["temp_normalized"]})
        self.learn_one({"value": data["temp_normalized"]})
        rr.log_scalar(f"temp_{data['sensor_id']}/data", data["temp"], color=[155, 155, 155])
        if data["score"] > 0.7:
            data["anom"] = True
            rr.log_point(f"3dpoint/anomaly/{data['sensor_id']}", [t, data["temp"], float(data['sensor_id'])], radius=0.3, color=[255,100,100])
            rr.log_scalar(
                f"temp_{data['sensor_id']}/data/anomaly",
                data["temp"],
                scattered=True,
                radius=3.0,
                color=[255, 100, 100],
            )
        else:
            data["anom"] = False
            rr.log_point(f"3dpoint/data/{data['sensor_id']}", [t, data["temp"], float(data['sensor_id'])], radius=0.1)
        return self, data


flow.stateful_map("detector", lambda: AnomalyDetector(), AnomalyDetector.update)
# (("fe7f93", {"timestamp":"2023..", "value":0.08, "score":0.02}))


# def format_output(key__data):
#     key, data = key__data
#     return (
#         f"{data['sensor_id']}: time = {data['ts']}, "
#         f"value = {data['temp']:.3f}, "
#         f"score = {data['score']:.2f}, "
#         f"is anomalous: {data['anom']}"
#     )

# flow.map(format_output)

flow.output("output", StdOutput())
