# pyhiveConnection
support hive zookeeper HA mode(based on pyhive and kazoo)) \
pass username,password,databasename,zkhosts,port and return pyhive DB-API cursor
# requirements
pyhive>=0.5 \
kazoo
# example
```python
from pyhive_connection import hive_connector
conn = hive_connector.connection("node15.test:2181,node16.test:2181","/hiveserver2","serverUri",username="foo", passwd="foo_passwd", database="foo_db")
cursor = conn.cursor()
cursor.execute("show databases")
print( cursor.fetchall() )
cursor.close()
conn.close() # make sure the cursor and conn closed
```
# kerberos example
```python
from pyhive_connection import hive_connector
conn = hive_connector.connection("node15.test:2181,node16.test:2181","/hiveserver2","serverUri",username="foo",auth='KERBEROS',
                           kerberos_service_name="hive", database="foo_db")
cursor = conn.cursor()
cursor.execute("show databases")
print( cursor.fetchall() )
cursor.close()
conn.close() # make sure the cursor and conn closed
```
