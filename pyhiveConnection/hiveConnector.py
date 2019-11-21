from kazoo.client import KazooClient
from pyhive import hive
import random
import logging

logger = logging.getLogger(__name__)


# for LUDP Connection
def ludp_connect(username, passwd, database):
    # for LUDP test environment
    zk_host = "node81.it.leap.com:2181,node82.it.leap.com:2181"
    znode_name = "/ludp_hive_ha"
    service_keyword = "serverUri"
    return connection(zk_host, znode_name, service_keyword, username=username, passwd=passwd, database=database)


# connect hive by pyhive and return cursor
def connection(zk_host, znode_name, service_keyword, **kwargs):
    host_list = discovery_thrift_service_host(zk_host, znode_name, service_keyword)
    host_length = len(host_list)
    random.seed()
    is_connected = False
    cursor = None
    while is_connected is False and host_length > 0:
        index = random.randint(0, host_length - 1)
        host_str = host_list.pop(index).split(":")
        try:
            cursor = hive.connect(host=host_str[0], port=host_str[1], **kwargs).cursor()
            is_connected = True
        except Exception as e:
            is_connected = False
            if host_length > 1:
                logger.warning("ERROR:Can not connect " + host_str[0] + ":" + host_str[1] + " .try another thrift server...")
            else:
                logger.error("ERROR:Can not connect hiveserver2, please check the connection config and the hiveserver")
                return 0
        host_length -= 1
    if cursor is None:
        raise Exception("No available HiveServer2 Connection")
    return cursor


# discovery the thrfit service host list
def discovery_thrift_service_host(zk_host, znode_name, service_keyword):
    zk_client = KazooClient(hosts=zk_host)
    zk_client.start()
    # get the children name of zonde
    result = zk_client.get_children(znode_name)
    # ['serverUri=salve91:10000;version=1.2.1000.2.6.3.0-235;sequence=0000000458']
    zk_client.stop()
    host_list = []
    for server in result:
        attrs = server.split(";")
        attr_map = {attr.split("=")[0]: attr.split("=")[1] for attr in attrs}
        uri = attr_map.get(service_keyword)
        if uri:
            host_list.append(uri)
    return host_list
