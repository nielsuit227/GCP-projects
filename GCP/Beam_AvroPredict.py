import io, logging, avro, joblib
import apache_beam as beam
from fastavro import schemaless_reader, parse_schema
from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions

logging.getLogger().setLevel(logging.INFO)
fields = [
    {'name': 'id', 'type': 'int'},
    {'name': 'ts', 'type': 'int'},
]
for i in range(50):
    fields.append({'name': 'sensorname' + str(i), 'type': 'float'})
raw_schema = {
    'type': 'record',
    'namespace': 'AvroPredict',
    'name': 'Entity',
    'fields': fields
}
PRJCT = 'archtrial'
TPC = "dataTopic"
topic_url = 'projects/{project}/topics/{topic}'.format(project=PRJCT, topic=TPC)
model_url = 'gs://dataflow-testing-42/models/svmtest.joblib'
buffer = 30


class JobOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--welcome', type=str)


class AvroReader:
    def __init__(self, schema):
        self.schema = schema
        self.reader = avro.io.DatumReader(self.schema)

    def decode(self, record):
        bytes = io.BytesIO(record)
        return schemaless_reader(bytes, self.schema)


class Predict(beam.DoFn):
    def __init__(self, model_dir):
        self.model_dir = model_dir
        self.session = None

    def process(self, element):
        if not self.session:
            self.session = joblib.load(self.model_dir)
        id, ts, feed = element
        results = self.session.decision_function([feed])[0]
        yield {
            'id': id,
            'ts': ts,
            'prediction': results.tolist()
        }


class Format(beam.DoFn):
    def process(self, element):
        devId, devData = element
        yield (
            devId,
            devData['ts'],
            [devData[key] for key in devData.keys()
             if key not in ['id', 'ts']]
        )


def run(arv=None, save_main_session=True):
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        numWorkers=12,
        project=PRJCT,
        job_name='printtemplate01',
        temp_location='gs://dataflow-testing-42/temp',
        region='us-central1',
        streaming=True,
        save_main_session=save_main_session,
    )

    p = beam.Pipeline(options=pipeline_options)

    schema = parse_schema(raw_schema)
    avroR = AvroReader(schema)
    new_lines = (
            p
            | 'Read' >> ReadFromPubSub(topic=topic_url).with_output_types(bytes)
            | 'Deserialize' >> beam.Map(lambda input: avroR.decode(input))
            | 'Format' >> beam.ParDo(Format())
            | 'Predict' >> beam.ParDo(Predict(model_url))
    )
    new_lines | 'Print' >> beam.Map(logging.info)

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()

