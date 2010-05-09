from exceptions import Exception
import socket
import threading
from Queue import Queue

try:
    import pkg_resources
    pkg_resources.require('Thrift')
except ImportError:
    pass
from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from cassandra import Cassandra
from cassandra.ttypes import AuthenticationRequest

__all__ = ['connect', 'connect_thread_local', 'NoServerAvailable']

DEFAULT_SERVER = 'localhost:9160'

class NoServerAvailable(Exception):
    pass

def create_client_transport(server, framed_transport, timeout, logins):
    host, port = server.split(":")
    socket = TSocket.TSocket(host, int(port))
    if timeout is not None:
        socket.setTimeout(timeout*1000.0)
    if framed_transport:
        transport = TTransport.TFramedTransport(socket)
    else:
        transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    transport.open()

    if logins is not None:
        for keyspace, credentials in logins.iteritems():
            request = AuthenticationRequest(credentials=credentials)
            client.login(keyspace, request)

    return client, transport

def connect(servers=None, framed_transport=False, timeout=None, logins=None):
    """
    Constructs a single Cassandra connection. Initially connects to the first
    server on the list.
    
    If the connection fails, it will attempt to connect to each server on the
    list in turn until one succeeds. If it is unable to find an active server,
    it will throw a NoServerAvailable exception.

    Parameters
    ----------
    servers : [server]
              List of Cassandra servers with format: "hostname:port"

              Default: ['localhost:9160']
    framed_transport: bool
              If True, use a TFramedTransport instead of a TBufferedTransport
    timeout: float
              Timeout in seconds (e.g. 0.5)

              Default: None (it will stall forever)
    logins : dict
              Dictionary of Keyspaces and Credentials

              Example: {'Keyspace1' : {'username':'jsmith', 'password':'havebadpass'}}

    Returns
    -------
    Cassandra client
    """

    if servers is None:
        servers = [DEFAULT_SERVER]
    return SingleConnection(servers, framed_transport, timeout, logins)

def connect_thread_local(servers=None, round_robin=True, framed_transport=False, timeout=None, logins=None):
    """
    Constructs a Cassandra connection for each thread. By default, it attempts
    to connect in a round_robin (load-balancing) fashion. Turn it off by
    setting round_robin=False

    If the connection fails, it will attempt to connect to each server on the
    list in turn until one succeeds. If it is unable to find an active server,
    it will throw a NoServerAvailable exception.

    Parameters
    ----------
    servers : [server]
              List of Cassandra servers with format: "hostname:port"

              Default: ['localhost:9160']
    round_robin : bool
              Balance the connections. Set to False to connect to each server
              in turn.
    framed_transport: bool
              If True, use a TFramedTransport instead of a TBufferedTransport
    timeout: float
              Timeout in seconds (e.g. 0.5 for half a second)

              Default: None (it will stall forever)
    logins : dict
              Dictionary of Keyspaces and Credentials

              Example: {'Keyspace1' : {'username':'jsmith', 'password':'havebadpass'}}

    Returns
    -------
    Cassandra client
    """

    if servers is None:
        servers = [DEFAULT_SERVER]
    return ThreadLocalConnection(servers, round_robin, framed_transport, timeout, logins)

class SingleConnection(object):
    def __init__(self, servers, framed_transport, timeout, logins):
        self._servers = servers
        self._client = None
        self._framed_transport = framed_transport
        self._timeout = timeout
        self._logins = logins if logins is not None else {}

    def login(self, keyspace, credentials):
        self._logins[keyspace] = credentials

    def __getattr__(self, attr):
        def client_call(*args, **kwargs):
            if self._client is None:
                self._find_server()
            try:
                return getattr(self._client, attr)(*args, **kwargs)
            except (Thrift.TException, socket.timeout, socket.error), exc:
                # Connection error, try to connect to all the servers
                self._transport.close()
                self._client = None

                for server in self._servers:
                    try:
                        self._client, self._transport = create_client_transport(server, self._framed_transport, self._timeout, self._logins)
                        return getattr(self._client, attr)(*args, **kwargs)
                    except (Thrift.TException, socket.timeout, socket.error), exc:
                        continue
                self._client = None
                raise NoServerAvailable()

        setattr(self, attr, client_call)
        return getattr(self, attr)

    def _find_server(self):
        for server in self._servers:
            try:
                self._client, self._transport = create_client_transport(server, self._framed_transport, self._timeout, self._logins)
                return
            except (Thrift.TException, socket.timeout, socket.error), exc:
                continue
        self._client = None
        raise NoServerAvailable()

class ThreadLocalConnection(object):
    def __init__(self, servers, round_robin, framed_transport, timeout, logins):
        self._servers = servers
        self._queue = Queue()
        for i in xrange(len(servers)):
            self._queue.put(i)
        self._local = threading.local()
        self._round_robin = round_robin
        self._framed_transport = framed_transport
        self._timeout = timeout
        self._logins = logins if logins is not None else {}

    def login(self, keyspace, credentials):
        self._logins[keyspace] = credentials

    def __getattr__(self, attr):
        def client_call(*args, **kwargs):
            if getattr(self._local, 'client', None) is None:
                self._find_server()

            try:
                return getattr(self._local.client, attr)(*args, **kwargs)
            except (Thrift.TException, socket.timeout, socket.error), exc:
                # Connection error, try to connect to all the servers
                self._local.transport.close()
                self._local.client = None

                servers = self._round_robin_servers()

                for server in servers:
                    try:
                        self._local.client, self._local.transport = create_client_transport(server, self._framed_transport, self._timeout, self._logins)
                        return getattr(self._local.client, attr)(*args, **kwargs)
                    except (Thrift.TException, socket.timeout, socket.error), exc:
                        continue
                self._local.client = None
                raise NoServerAvailable()

        setattr(self, attr, client_call)
        return getattr(self, attr)

    def _round_robin_servers(self):
        servers = self._servers
        if self._round_robin:
            i = self._queue.get()
            self._queue.put(i)
            servers = servers[i:]+servers[:i]

        return servers

    def _find_server(self):
        servers = self._round_robin_servers()

        for server in servers:
            try:
                self._local.client, self._local.transport = create_client_transport(server, self._framed_transport, self._timeout, self._logins)
                return
            except (Thrift.TException, socket.timeout, socket.error), exc:
                continue
        self._local.client = None
        raise NoServerAvailable()
