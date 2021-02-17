import io
import time

import apache_beam as beam
import numpy as np
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from fastavro import parse_schema, schemaless_reader, schemaless_writer

FEATURES = 50


class AvroReader:
    def __init__(self, schema):
        self.schema = schema

    def decode(self, record):
        bytes = io.BytesIO(record)
        return schemaless_reader(bytes, self.schema)

    def encode(self, record):
        bytes = io.BytesIO()
        schemaless_writer(bytes, self.schema, record)
        return bytes.getvalue()


class Format(beam.DoFn):
    def process(self, element):
        devId, devData = element
        devData.sort(key=lambda item: item["ts"])
        sensor1 = [x["sensor1"] for x in devData]
        sensor2 = [x["sensor2"] for x in devData]
        yield {
            "devId": devId,
            "sensor1": sensor1,
            "sensor2": sensor2,
        }


def DataProducer(avroReader, devices=5, samples=100):
    data = []
    for sample in range(samples):
        for device in range(devices):
            message = {
                "id": device,
                "ts": int(time.time() + sample),
                "sensor1": np.random.rand(),
                "sensor2": np.random.rand(),
            }
            data.append(avroReader.encode(message))
    return data


fields = [
    {"name": "id", "type": "int"},
    {"name": "ts", "type": "int"},
    {"name": "sensor1", "type": "float"},
    {"name": "sensor2", "type": "float"},
]
schema = parse_schema(
    {"type": "record", "namespace": "AmploAmbibox", "name": "sample", "fields": fields}
)
avroReader = AvroReader(schema)
data = DataProducer(avroReader)
pipeline_options = PipelineOptions(streaming=True)

with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        | "Extract" >> beam.Create(DataProducer(avroReader))
        | "Decode" >> beam.Map(lambda input: avroReader.decode(input))
        | "Add Timestamp" >> beam.Map(lambda x: window.TimestampedValue(x, x["ts"]))
        | "Window"
        >> beam.WindowInto(
            beam.transforms.window.SlidingWindows(size=10, period=1),
            allowed_lateness=20,
        )
        | "Group Devs" >> beam.GroupBy(lambda elem: elem["id"])
        | "Format" >> beam.ParDo(Format())
        | "Print" >> beam.Map(print)
    )
