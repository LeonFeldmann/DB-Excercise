import psycopg2 as psycopg2
import json
import datetime

dt_string = datetime.datetime.utcnow().strftime(
    '%d.%m.%Y %H:%M:%S.%f')[:-3]

# Connect to your postgres DB
try:
    conn = psycopg2.connect("dbname='iot' host='localhost'")
except:
    print("I am unable to connect to the database")


# Open a cursor to perform database operations
cur = conn.cursor()

cur.execute("""
            CREATE TABLE iot(
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            data json
            )
            """
            )
# Execute a query
# cur.execute("""
#             CREATE TABLE test2(
#             data VARCHAR(255),
#             something_else VARCHAR(255)
#             )
#             """
#             )

# cur.execute("""
# INSERT INTO test2 (data, something_else)
# VALUES (12345, 'something')
# """)

# initialize the db initdb /Users/joachimgoennheimer/Desktop/5.Semester/IOT/mqttDBUebung/DB-Excercise/data
# start postgress with postgres -D /Users/joachimgoennheimer/Desktop/5.Semester/IOT/mqttDBUebung/DB-Excercise/data


def save_message(message):
    message_json = message.payload
    decoded_str = message_json.decode('utf-8')
    decoded_json = json.dumps(decoded_str)
    # print(decoded_json)
    # print(str(message_json))
    # print(type(message_json))
    cur.execute("""
    INSERT INTO iot (timestamp, data)
VALUES(current_timestamp, '""" + decoded_json + """');
    """)
    cur.execute("SELECT * FROM iot order by id desc limit 6")
    records = cur.fetchall()

    # print(records)
    for record in records:
        insert_time = record[1]
        record_json = json.loads(record[2])
        publish_time = record_json["time"]
        print("insert_time: %s, publish_time %s", insert_time, publish_time)


text_json = '{ "customer": "John Doe", "items": {"product": "Beer","qty": 6}}'

# cur.execute("""
# INSERT INTO iot (timestamp, data)
# VALUES(CURRENT_TIMESTAMP, '""" + text_json + """');
# """)

# cur.execute("INSERT INTO iot (" + dt_string + ", " + json + ")")

# cur.execute("""
# INSERT INTO iot
# (NOW(), """ + json + """)
# """)

cur.execute("SELECT * FROM iot")

# Retrieve query results
records = cur.fetchall()
print(records)
