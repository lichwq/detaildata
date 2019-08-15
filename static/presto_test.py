
import prestodb

import datetime

conn=prestodb.dbapi.connect(
    host='127.0.0.1',
    port=10028,
    user='the-user',
    catalog='kafka',
    schema='default',
)

cur = conn.cursor()
# cur.execute("select json_extract_scalar(_message, '$.partnerId') AS partnerId,count(1),sum(cast(json_extract_scalar(_message, '$.settlementAmount') as double)) from prestotestv2 where json_extract_scalar(_message, '$.date') between '2019-06-05 18:29:00' and '2019-06-05 18:30:00' group by json_extract_scalar(_message, '$.partnerId')")
# rows = cur.fetchall()

# sql_str = "select count(1) from prestotestv2 where json_extract_scalar(_message, '$.date') between '2019-06-05 18:29:00' and '2019-06-05 18:30:00'"

sql_str = "select count(1) from prestotest01"

# sql_str = "select count(1) from prestotest01 group by "

starttime = datetime.datetime.now()

cur.execute(sql_str)

rows = cur.fetchall()

print rows

endtime = datetime.datetime.now()

print (endtime - starttime).seconds