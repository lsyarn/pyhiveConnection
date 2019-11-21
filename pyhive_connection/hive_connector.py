from kazoo.client import KazooClient
from pyhive import hive
import random
import logging

logger = logging.getLogger(__name__)


def connection(zk_host, zk_path='/hiveserver2', service_keyword='serverUri', **kwargs):
    """
    connect hive by pyhive and return cursor
    :param zk_host: zookeeper host:port, delimited by `,`
    :param zk_path: Path of node to list, default `/hiveserver2`
    :param service_keyword: keyword for hiveserver2 server uri, default `serverUri`
    :param kwargs: kwargs passed to pyhive.hive.connect
    :return:
    """
    host_list = _discovery_thrift_service_host(zk_host, zk_path, service_keyword)
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
            logger.debug(e)
            is_connected = False
            if host_length > 1:
                logger.warning("ERROR:Can not connect %s:%s.try another thrift server...", host_str[0], host_str[1])
            else:
                logger.error("ERROR:Can not connect hiveserver2, please check the connection config and the hiveserver")
                return 0
        host_length -= 1
    if cursor is None:
        raise Exception("No available HiveServer2 Connection")
    return cursor


def _discovery_thrift_service_host(zk_host, zk_path='/hiveserver2', service_keyword='serverUri'):
    """
    discovery the thrfit service host list
    :param zk_host: zookeeper host:port, delimited by `,`
    :param zk_path: Path of node to list, default `/hiveserver2`
    :param service_keyword: keyword for hiveserver2 server uri, default `serverUri`
    :return: host:port list for hiveserver2
    """
    zk_client = KazooClient(hosts=zk_host)
    zk_client.start()
    # get the children name of zonde
    result = zk_client.get_children(zk_path)
    # result is something like ['serverUri=salve91:10000;version=1.2.1000.2.6.3.0-235;sequence=0000000458']
    zk_client.stop()
    host_list = []
    for server in result:
        attrs = server.split(";")
        attr_map = {attr.split("=")[0]: attr.split("=")[1] for attr in attrs}
        uri = attr_map.get(service_keyword)
        if uri:
            host_list.append(uri)
    return host_list
