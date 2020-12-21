import sys
from flask import escape


# [START functions_helloworld_pubsub]
def hello_pubsub(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    import base64

    print("""This Function was triggered by messageId {} published at {}
    """.format(context.event_id, context.timestamp))

    if 'data' in event:
        name = base64.b64decode(event['data']).decode('utf-8')
    else:
        name = 'World'
    print('Hello {}!'.format(name))
# [END functions_helloworld_pubsub]


def to_postgres(event, context):
    import base64, sqlalchemy, os, sys
    from datetime import datetime
    try:
        json_string = base64.b64decode(event['data']).decode('utf-8')[1:-1]
        data = {i.split(': ')[0]: i.split(': ')[1] for i in json_string.split(', ')}
        data['start'] = datetime.fromtimestamp(float(data['start']))
        data['end'] = datetime.fromtimestamp(float(data['end']))
        print(data)
        # Connection
        DB_USER = os.environ.get('DB_USER')
        DB_PASS = os.environ.get('DB_PASS')
        DB_NAME = os.environ.get('DB_NAME')
        DB_CONN = os.environ.get('DB_CONN')
        engine = sqlalchemy.create_engine(
            sqlalchemy.engine.url.URL(
                drivername="postgresql+pg8000",
                username=DB_USER,
                password=DB_PASS,
                database=DB_NAME,
                query={
                    "unix_sock": "/cloudsql/{}/.s.PGSQL.5432".format(DB_CONN)
                }
            )
        )
        if type(engine) == 'str':
            print(engine)
        connection = engine.connect()

        # Check Order id, start_ts
        query = 'SELECT id, actual_start, progress FROM app_order WHERE riga={}'.format(data['order'])
        orderQuery = connection.execute(query)
        result = orderQuery.fetchall()

        # Create Order if it doesn't exist
        if len(result) == 0:
            # Find max id
            query = connection.execute("SELECT MAX(id) FROM app_order;")
            order_id = query.fetchall()[0][0] + 1
            # Insert new order
            query = "INSERT INTO app_order (id, riga, status, priority, actual_start, actual_end, progress, suit_id) VALUES ({}, {}, 2, 2, '{}', '{}', {}, {})" \
                .format(order_id, data['order'], data['start'], data['end'], data['quantity'], data['suit'])
            connection.execute(query)

        # Update existing order
        else:
            order_id, order_start, progress = result[0]
            # Add actual start
            if order_start == None:
                query = "UPDATE app_order SET actual_start='{}', actual_end='{}', progress={} where id={}".format(
                    data['start'], data['end'], progress + int(data['quantity']), order_id)
                connection.execute(query)
            # Or let it remain if it's already there
            else:
                query = "UPDATE app_order SET actual_end='{}', progress={} where id={}".format(
                    data['end'], progress + int(data['quantity']), order_id)
                connection.execute(query)

        # Add event
        query = "INSERT INTO app_event (start_ts, end_ts, employee_id, task_id, quantity, order_id) VALUES ('{}', '{}', '{}', '{}', {}, {})" \
            .format(data['start'], data['end'], data['employee'], data['task'], data['quantity'], order_id)
        connection.execute(query)
    except Exception as e:
        exc_type, exc_obj, tb = sys.exc_info()
        print(tb.tb_lineno)
        print(e)