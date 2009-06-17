import time

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

def _get_user(client, user_id):
    try:
        cols = client.get_slice('TwitterClone', str(user_id), 'users', -1, -1)
    except (NotFoundException, IndexError):
        return None
    user = {}
    for col in cols:
        user[col.columnName] = col.value.decode('utf-8')
    return user

get_user = with_thrift()(_get_user)


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


def _get_timeline(client, user_id, offset=0, limit=20):
    try:
        cols = client.get_slice_super('TwitterClone', str(user_id),
            'tweet_edges:user_tweets', offset, limit)[0].columns
    except (NotFoundException, IndexError):
        return []
    return [_get_tweet(client, t.value) for t in cols]

get_timeline = with_thrift()(_get_timeline)


def _get_tweets(client, user_id, offset=0, limit=20):
    try:
        cols = client.get_slice_super('TwitterClone', str(user_id),
            'tweet_edges:friend_tweets', offset, limit)[0].columns
    except (NotFoundException, IndexError):
        return []
    return [_get_tweet(client, t.value) for t in cols]

get_tweets = with_thrift()(_get_tweets)


def _friend_user(client, from_user_id, to_user_id, block=True):
    tdt = time.time()
    
    friend_key = 'user_edges:friends:%s' % (str(to_user_id),)
    client.insert('TwitterClone', str(from_user_id), friend_key,
        str(to_user_id), tdt, block)

    followers_key = 'user_edges:followers:%s' % (str(from_user_id),)
    client.insert('TwitterClone', str(to_user_id), friend_key,
        str(from_user_id), tdt, block)

friend_user = with_thrift()(_friend_user)


def _unfriend_user(client, from_user_id, to_user_id, block=True):
    friend_key = 'user_edges:friends:%s' % (str(to_user_id),)
    try:
        col = client.get_superColumn('TwitterClone', str(from_user_id),
            friend_key).columns[0]
        client.remove('TwitterClone', str(from_user_id), friend_key,
            col.timestamp, block)
    except (NotFoundException, IndexError):
        pass
    
    followers_key = 'user_edges:followers:%s' % (str(from_user_id),)
    try:
        col = client.get_superColumn('TwitterClone', str(to_user_id),
            followers_key).columns[0]
        client.remove('TwitterClone', str(to_user_id), followers_key,
            col.timestamp, block)
    except (NotFoundException, IndexError):
        pass

unfriend_user = with_thrift()(_unfriend_user)


def _tweet(client, from_user_id, tweet):
    pass