import io, logging, avro
import apache_beam as beam
from fastavro import schemaless_reader, parse_schema
from apache_beam.io import ReadFromPubSub
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions


logging.getLogger().setLevel(logging.INFO)
raw_schema = {
    'type': 'record',
    'namespace': 'AvroPubSubPrint',
    'name': 'Entity',
    'fields': [
        {"name": "id", "type": "int"},
        {"name": "ts", "type": "int"},
        {"name": "sensor1",  "type": ["float", "null"]},
        {"name": "sensor2", "type": ["float", "null"]}
    ]
}
PRJCT = 'archtrial'
TPC = "dataTopic"
topic_url = 'projects/{project}/topics/{topic}'.format(project=PRJCT, topic=TPC)
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

    def deserialize(self, record):
        bytes = io.BytesIO(record)
        binary = avro.io.BinaryDecoder(bytes)
        return self.reader.read(binary)

class Format(beam.DoFn):
    def process(self, element):
        devId, devData = element
        devData.sort(key=lambda item: item['ts'])
        sensor1 = [x['sensor1'] for x in devData]
        sensor2 = [x['sensor2'] for x in devData]
        yield {
            'id': devId,
            'sensor1': sensor1,
            'sensor2': sensor2
        }


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
    job_options = pipeline_options.view_as(JobOptions)


    p = beam.Pipeline(options=pipeline_options)

    schema = parse_schema(raw_schema)
    avroR = AvroReader(schema)
    new_lines = (
        p
        | 'Read' >> ReadFromPubSub(topic=topic_url).with_output_types(bytes)
        | 'Deserialize' >> beam.Map(lambda input: avroR.decode(input))
        | 'Add Timestamp' >> beam.Map(lambda x: window.TimestampedValue(x, x['ts']))
        | 'Window' >> beam.WindowInto(beam.transforms.window.SlidingWindows(size=10, period=1))
        | 'Group' >> beam.GroupBy(lambda elem: elem['id'])
        | 'Format' >> beam.ParDo(Format())
        # Need a sorting too...
    )
    new_lines | 'Print' >> beam.Map(logging.info)

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()