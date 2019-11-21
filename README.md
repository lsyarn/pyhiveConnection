# pyhiveConnection
support hive zookeeper HA mode(based on pyhive and kazoo)) \
pass username,password,databasename,zkhosts,port and return pyhive DB-API cursor
# requirements
pyhive>=0.5 \
kazoo
# example
```python
from pyhiveConnection import hiveConnector
cursor = hiveConnector.connection("node15.test:2181,node16.test:2181","/hiveserver2","serverUri",username="foo", passwd="foo_passwd", database="foo_db")
cursor.execute("show databases")
print( curosr.fetchall() )
```
# kerberos example
```python
from pyhiveConnection import hiveConnector
cursor = hiveConnector.connection("node15.test:2181,node16.test:2181","/hiveserver2","serverUri",username="foo",auth='KERBEROS',
                           kerberos_service_name="hive", database="foo_db")
cursor.execute("show databases")
print( curosr.fetchall() )
```
