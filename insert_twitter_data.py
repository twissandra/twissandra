import rfc822
import datetime
import calendar
import urllib2
import json
import time

from collections import defaultdict

from utils import with_thrift, get_followers

from cassandra.ttypes import batch_mutation_t, column_t, superColumn_t

USERS = ['ericflo', 'eston', 'kneath', 'mihasya', 'mjmalone', 'rcrowley',
    'thauber']

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
        
    for user in users:
        columns = []
        user_id = str(user['id'])
        for key, value in user.items():
            columns.append(column_t(
                columnName=key,
                value=unicode(value).encode('utf-8'),
                timestamp=user['created_at_in_seconds']
            ))
        if columns:
            client.batch_insert(batch_mutation_t(
                table='TwitterClone',
                key=user_id,
                cfmap={'users': columns},
            ), True)
        client.insert('TwitterClone', user['screen_name'], 'usernames:id',
            user_id, user['created_at_in_seconds'], True)

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
        if follower_columns:
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
        if friend_columns:
            supercolumns.append(superColumn_t(
                name='friends',
                columns=friend_columns
            ))
        
        if supercolumns:
            client.batch_insert_superColumn(batch_mutation_t(
                table='TwitterClone',
                key=str(data['id']),
                cfmap={'user_edges': supercolumns}
            ), True)

@with_thrift()
def import_tweets(client, usernames):
    for username in usernames:
        print 'Importing tweets for %s' % (username,)
        url = 'http://twitter.com/statuses/user_timeline/%s.json' % (username,)
        
        user_tweet_columns = []
        user_id = None
        followers = None
        follower_columns = defaultdict(lambda: [])
        
        tweets = json.loads(urllib2.urlopen(url).read())
        
        for tweet in tweets:
            user = tweet.pop('user', None)
            
            if user_id is None:
                user_id = str(user['id'])
                followers = map(str, get_followers(user_id))
            
            tweet['user_id'] = user['id']
            
            parsed_date = rfc822.parsedate(tweet['created_at'])
            created_at_in_seconds = calendar.timegm(parsed_date)
            tweet['created_at'] = datetime.datetime.fromtimestamp(
                created_at_in_seconds)
            tweet['created_at_in_seconds'] = created_at_in_seconds
            
            tweet_id = str(tweet['id'])
            columns = []
            for key, value in tweet.items():
                columns.append(column_t(
                    columnName=key,
                    value=unicode(value).encode('utf-8'),
                    timestamp=created_at_in_seconds
                ))
            
            if columns:
                client.batch_insert(batch_mutation_t(
                    table='TwitterClone',
                    key=tweet_id,
                    cfmap={'tweets': columns},
                ), True)
            
            user_tweet_columns.append(column_t(
                columnName=tweet_id,
                value=tweet_id,
                timestamp=created_at_in_seconds
            ))
            
            for follower in followers:
                follower_columns[follower].append(column_t(
                    columnName=tweet_id,
                    value=tweet_id,
                    timestamp=created_at_in_seconds
                ))
        
        for follower, columns in follower_columns.iteritems():
            supercolumn = superColumn_t(
                name='friend_tweets',
                columns=columns
            )
            client.batch_insert_superColumn(batch_mutation_t(
                table='TwitterClone',
                key=follower,
                cfmap={'tweet_edges': [supercolumn]}
            ), True)
        
        if user_tweet_columns:
            supercolumn = superColumn_t(
                name='user_tweets',
                columns=user_tweet_columns
            )
            client.batch_insert_superColumn(batch_mutation_t(
                table='TwitterClone',
                key=user_id,
                cfmap={'tweet_edges': [supercolumn]}
            ), True)

def main():
    import_users(USERS)
    import_social_graph(USERS)
    import_tweets(USERS)

if __name__ == '__main__':
    main()