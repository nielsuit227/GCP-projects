import io, numpy, sys
from fastavro import parse_schema, schemaless_writer, schemaless_reader


FEATURES = 50

fields = []
message = {}
for i in range(FEATURES):
    fields.append({'name': 'sensornamejadajada' + str(i), 'type': 'float'})
    message['sensornamejadajada' + str(i)] = numpy.random.rand()
schema = parse_schema(
    {
        'type': 'record',
        'namespace': 'AmploAmbibox',
        'name': 'sample',
        'fields': fields
    }
)
bytes_i = io.BytesIO()
schemaless_writer(bytes_i, schema, message)
encoded = bytes_i.getvalue()
print(sys.getsizeof(encoded))

bytes = io.BytesIO(encoded)
print(schemaless_reader(bytes, schema))
