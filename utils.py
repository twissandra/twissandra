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


def _get_user_id(client, username):
    try:
        col = client.get_column('TwitterClone', username, 'usernames:id')
    except NotFoundException:
        return None
    return col.value

get_user_id = with_thrift()(_get_user_id)


def _get_friends(client, user_id):
    try:
        cols = client.get_slice_super('TwitterClone', str(user_id),
            'user_edges:friends', -1, -1)[0].columns
    except (NotFoundException, IndexError):
        return None
    return [int(c.value) for c in cols]

get_friends = with_thrift()(_get_friends)


def _get_followers(client, user_id):
    try:
        cols = client.get_slice_super('TwitterClone', str(user_id),
            'user_edges:followers', -1, -1)[0].columns
    except (NotFoundException, IndexError):
        return None
    return [int(c.value) for c in cols]
    
get_followers = with_thrift()(_get_followers)


def _get_tweet(client, tweet_id):
    try:
        cols = client.get_slice('TwitterClone', str(tweet_id), 'tweets', -1, -1)
    except (NotFoundException, IndexError):
        return None
    tweet = {}
    for col in cols:
        tweet[col.columnName] = col.value.decode('utf-8')
    return tweet

get_tweet = with_thrift()(_get_tweet)


def _get_timeline(client, user_id):
    try:
        cols = client.get_slice_super('TwitterClone', str(user_id),
            'tweet_edges:user_tweets', -1, -1)[0].columns
    except (NotFoundException, IndexError):
        return []
    return [_get_tweet(client, t.value) for t in cols]

get_timeline = with_thrift()(_get_timeline)

def _get_tweets(client, user_id):
    try:
        cols = client.get_slice_super('TwitterClone', str(user_id),
            'tweet_edges:friend_tweets', -1, -1)[0].columns
    except (NotFoundException, IndexError):
        return []
    return [_get_tweet(client, t.value) for t in cols]

get_tweets = with_thrift()(_get_tweets)