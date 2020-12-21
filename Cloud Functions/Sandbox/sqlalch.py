import sqlalchemy
from datetime import datetime

data = {'employee': 'Bettola Simona', 'order': '2222', 'task': 'Hood top', 'machine': '5', 'quantity': '1', 'start': '1606753085750'}


engine = sqlalchemy.create_engine(
    sqlalchemy.engine.url.URL(
        drivername="postgresql",
        username='postgres',
        password='/=A:~84%S2w`Y8=^',
        host='127.0.0.1',
        port='5432',
        database='responderdb'
    )
)
connection = engine.connect()
# keys = ['order', 'task', 'employee', 'quantity', 'start', 'end']
# query = "INSERT INTO app_event (order_id, task_id, employee_id, quantity, start_ts, end_ts) VALUES (%s, %s, %s, %s, %s, %s);" % \
#     tuple([data[key] for key in keys])
# order_query = connection.execute(query)
# order_id = order_query.fetchall()[0][0]
# employee_query = connection.execute("SELECT id FROM app_employee WHERE name='%s'" % data['employee'])
# employee_id = employee_query.fetchall()[0][0]
# task_query = connection.execute()


data = {
    'order': 2000,
    'start': 1601304912.012,
    'end': 1601305912.012,
    'quantity': 16,
    'employee': 3,
    'task': 4,
    'suit': 2
}
data['start'] = datetime.fromtimestamp(data['start'])
data['end'] = datetime.fromtimestamp(data['end'])
# Get id & start
query = 'SELECT id, actual_start FROM app_order WHERE riga={}'.format(data['order'])
orderQuery = connection.execute(query)
result = orderQuery.fetchall()
# Create if not exist
if len(result) == 0:

    query = "INSERT INTO app_order (riga, status, priority, actual_start, actual_end, progress, suit_id) VALUES ({}, 2, 2, '{}', '{}', {}, {})" \
            .format(data['order'], data['start'], data['end'], data['quantity'], data['suit'])
    connection.execute(query)
    query = 'SELECT id FROM app_order WHERE riga={}'.format(data['order'])
    order_id = connection.execute(query).fetchall()[0][0]

# jaja
else:
    order_id, order_start = result[0]
    if order_start == None:
        query = "UPDATE app_order SET actual_start='{}' where id={}".format(datetime.fromtimestamp(data['start']), order_id)
        connection.execute(query)
query = "INSERT INTO app_event (start_ts, end_ts, employee_id, task_id, quantity, order_id) VALUES ('{}', '{}', '{}', '{}', {}, {})" \
        .format(data['start'], data['end'], data['employee'], data['task'], data['quantity'], order_id)
connection.execute(query)

