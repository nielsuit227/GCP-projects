import io, time, joblib
import numpy as np
import apache_beam as beam
from fastavro import parse_schema, schemaless_reader, schemaless_writer
from apache_beam.options.pipeline_options import PipelineOptions


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
        yield (devId, devData['ts'], [devData[key] for key in devData.keys() if key not in ['id', 'ts']])


class Predict(beam.DoFn):
    def __init__(self, model_dir):
        self.model_dir = model_dir
        self.session = None

    def process(self, element):
        if not self.session:
            print('Session started!')
            self.session = joblib.load(self.model_dir)
        id, ts, feed = element
        results = self.session.decision_function([feed])[0]
        yield {
            'id': id,
            'ts': ts,
            'prediction': results.tolist()
        }


def DataProducer(avroReader, devices=5, samples=100):
    data = []
    for sample in range(samples):
        for device in range(devices):
            message = {
                'id': device,
                'ts': int(time.time() + sample),
                # 'sensor1': np.random.rand(),
                # 'sensor2': np.random.rand()
            }
            for i in range(FEATURES):
                message['sensorname' + str(i)] = np.random.rand()
            data.append(
                avroReader.encode(message)
            )
    return data


def run():
    fields = [
        {'name': 'id', 'type': 'int'},
        {'name': 'ts', 'type': 'int'},
        # {'name': 'sensor1', 'type': 'float'},
        # {'name': 'sensor2', 'type': 'float'}
    ]
    for i in range(FEATURES):
        fields.append({'name': 'sensorname' + str(i), 'type': 'float'})
    schema = parse_schema({
        'type': 'record',
        'namespace': 'AmploAmbibox',
        'name': 'sample',
        'fields': fields
    })
    avroReader = AvroReader(schema)
    pipeline_options = PipelineOptions(
        streaming=True,
        save_main_session=True,
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Extract" >> beam.Create(DataProducer(avroReader))
            | "Decode" >> beam.Map(lambda input: avroReader.decode(input))
            | "Group Devs" >> beam.Map(lambda input: (input['id'], input))
            | "Format" >> beam.ParDo(Format())
            | "Predict" >> beam.ParDo(Predict('../svmtimer.joblib'))
            | 'Print' >> beam.Map(print)
        )


if __name__ == '__main__':
    print('Running!')
    run()