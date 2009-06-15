import rfc822
import datetime
import calendar
import urllib2
import json
import time

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

from cassandra import Cassandra
from cassandra.ttypes import batch_mutation_t, column_t, superColumn_t

USERS = ['ericflo', 'eston', 'kneath', 'mihasya', 'mmalone', 'rcrowley',
    'thauber']

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
def import_users(client, usernames):
    UNWANTED_KEYS = ('status', 'favourites_count', 'followers_count',
        'following', 'friends_count', 'statuses_count')
        
    users = []
    
    for username in usernames:
        print 'Importing user %s' % (username,)
        url = 'http://twitter.com/users/show/%s.json' % (username,)
        data = json.loads(urllib2.urlopen(url).read())
        
        parsed_date = rfc822.parsedate(data['created_at'])
        created_at_in_seconds = calendar.timegm(parsed_date)
        data['created_at'] = datetime.datetime.fromtimestamp(
            created_at_in_seconds)
        data['created_at_in_seconds'] = created_at_in_seconds
        
        for key in UNWANTED_KEYS:
            data.pop(key, None)
        
        users.append(data)
    
    users.sort(key=lambda u: u['created_at'])
    
    tt = time.time()
    
    for user in users:
        columns = []
        user_id = str(user['id'])
        for key, value in user.items():
            columns.append(column_t(
                columnName=key,
                value=str(value),
                timestamp=tt
            ))
        client.batch_insert(batch_mutation_t(
            table='TwitterClone',
            key=user_id,
            cfmap={'users': columns},
        ), True)
        client.insert('TwitterClone', user['screen_name'], 'usernames:id',
            user_id, tt, True)

@with_thrift()
def import_social_graph(client, usernames):    
    tt = time.time()
    
    for username in usernames:
        print 'Importing social graph for %s' % (username,)
        url = 'http://twitter.com/users/show/%s.json' % (username,)
        data = json.loads(urllib2.urlopen(url).read())
        
        supercolumns = []
        
        followers_url = 'http://twitter.com/followers/ids/%s.json' % (username,)
        followers = json.loads(urllib2.urlopen(followers_url).read())
        
        follower_columns = []
        for follower in followers:
            follower_columns.append(column_t(
                columnName=str(follower),
                value=str(follower),
                timestamp=tt
            ))
        supercolumns.append(superColumn_t(
            name='followers',
            columns=follower_columns
        ))
        
        friends_url = 'http://twitter.com/friends/ids/%s.json' % (username,)
        friends = json.loads(urllib2.urlopen(friends_url).read())
        
        friend_columns = []
        for friend in friends:
            friend_columns.append(column_t(
                columnName=str(friend),
                value=str(friend),
                timestamp=tt
            ))
        supercolumns.append(superColumn_t(
            name='friends',
            columns=friend_columns
        ))
    
        client.batch_insert_superColumn(batch_mutation_t(
            table='TwitterClone',
            key=str(data['id']),
            cfmap={'user_edges': supercolumns}
        ), True)
        

def main():
    import_users(USERS)
    import_social_graph(USERS)

if __name__ == '__main__':
    main()