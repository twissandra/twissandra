import sys

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

from cassandra import Cassandra
from cassandra.ttypes import NotFoundException

def with_thrift(host='localhost', port=9160):
    # TODO: Connection pooling, reuse, etc.
    def _inner(func):
        def __inner(*args, **kwargs):
            socket = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Cassandra.Client(protocol)
            val = None
            try:
                transport.open()
                val = func(client, *args, **kwargs)
            finally:
                transport.close()
            return val
        return __inner
    return _inner

@with_thrift()
def get_id(client, username):
    try:
        col = client.get_column('TwitterClone', username, 'usernames:id')
    except NotFoundException:
        return None
    return col.value

@with_thrift()
def get_friends(client, user_id):
    try:
        cols = client.get_slice_super('TwitterClone', str(user_id),
            'user_edges:friends', -1, -1)[0].columns
    except (NotFoundException, IndexError):
        return None
    return [int(c.value) for c in cols]

@with_thrift()
def get_followers(client, user_id):
    try:
        cols = client.get_slice_super('TwitterClone', str(user_id),
            'user_edges:followers', -1, -1)[0].columns
    except (NotFoundException, IndexError):
        return None
    return [int(c.value) for c in cols]

def main():
    username = sys.argv[1]
    
    user_id = get_id(username)
    if user_id is None:
        print 'Unable to find data for user %s' % (username,)
        return None
    
    friends = get_friends(user_id)
    followers = get_followers(user_id)
    
    print "FRIENDS:"
    print friends
    print '--------------------'
    print "FOLLOWERS:"
    print followers
    print '--------------------'

if __name__ == '__main__':
    main()