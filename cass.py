import struct
import time
import uuid

from odict import OrderedDict

import pycassa

from cassandra.ttypes import NotFoundException

try:
    import simplejson as json
except ImportError:
    import json

__all__ = ['get_user_by_id', 'get_user_by_username', 'get_friend_ids',
    'get_follower_ids', 'get_users_for_user_ids', 'get_friends',
    'get_followers', 'get_timeline', 'get_userline', 'get_tweet', 'save_user',
    'save_tweet', 'add_friends', 'remove_friends', 'DatabaseError',
    'NotFound', 'InvalidDictionary', 'PUBLIC_USERLINE_KEY']

CLIENT = pycassa.connect_thread_local(framed_transport=True)

USER = pycassa.ColumnFamily(CLIENT, 'Twissandra', 'User',
    dict_class=OrderedDict)
USERNAME = pycassa.ColumnFamily(CLIENT, 'Twissandra', 'Username',
    dict_class=OrderedDict)
FRIENDS = pycassa.ColumnFamily(CLIENT, 'Twissandra', 'Friends',
    dict_class=OrderedDict)
FOLLOWERS = pycassa.ColumnFamily(CLIENT, 'Twissandra', 'Followers',
    dict_class=OrderedDict)
TWEET = pycassa.ColumnFamily(CLIENT, 'Twissandra', 'Tweet',
    dict_class=OrderedDict)
TIMELINE = pycassa.ColumnFamily(CLIENT, 'Twissandra', 'Timeline',
    dict_class=OrderedDict)
USERLINE = pycassa.ColumnFamily(CLIENT, 'Twissandra', 'Userline',
    dict_class=OrderedDict)

PUBLIC_USERLINE_KEY = '!PUBLIC!'


class DatabaseError(Exception):
    pass


class NotFound(DatabaseError):
    pass


class InvalidDictionary(DatabaseError):
    pass


def _long(i):
    return struct.pack('>d', long(i))

def _unlong(b):
    return struct.unpack('>d', b)

def _get_friend_or_follower_ids(cf, user_id, count):
    try:
        friends = cf.get(str(user_id), column_count=count)
    except NotFoundException:
        return []
    return friends.keys()

def _get_line(cf, user_id, start, limit):
    start = _long(start) if start else ''
    try:
        timeline = cf.get(str(user_id), column_start=start, column_count=limit,
            column_reversed=True)
    except NotFoundException:
        return []
    tweets = TWEET.multiget(timeline.values())
    tweets = dict(((json.loads(t['id']), t) for t in tweets.values()))
    decoded = []
    for tweet_id in timeline.values():
        tweet = tweets.get(tweet_id)
        if not tweet:
            continue
        decoded.append(
            dict(((k, json.loads(v)) for k, v in tweet.iteritems()))
        )
    users = get_users_for_user_ids([u['user_id'] for u in decoded])
    users = dict(((u['id'], u) for u in users))
    for tweet in decoded:
        tweet['user'] = users.get(tweet['user_id'])
    return decoded


# QUERYING APIs

def get_user_by_id(user_id):
    try:
        user = USER.get(str(user_id))
    except NotFoundException:
        raise NotFound('User %s not found' % (user_id,))
    return dict(((k, json.loads(v)) for k, v in user.iteritems()))

def get_user_by_username(username):
    try:
        record = USERNAME.get(username.encode('utf-8'))
    except NotFoundException:
        raise NotFound('User %s not found' % (username,))
    if 'id' not in record:
        raise NotFound('Username %s not found' % (username,))
    return get_user_by_id(record['id'])

def get_friend_ids(user_id, count=5000):
    return _get_friend_or_follower_ids(FRIENDS, user_id, count)

def get_follower_ids(user_id, count=5000):
    return _get_friend_or_follower_ids(FOLLOWERS, user_id, count)

def get_users_for_user_ids(user_ids):
    try:
        users = USER.multiget(map(str, user_ids))
    except NotFoundException:
        raise NotFound('Users %s not found' % (user_ids,))
    decoded = []
    for user in users.values():
        decoded.append(
            dict(((k, json.loads(v)) for k, v in user.iteritems()))
        )
    return decoded

def get_friends(user_id, count=5000):
    friend_ids = get_friend_ids(user_id, count=count)
    return get_users_for_user_ids(friend_ids)

def get_followers(user_id, count=5000):
    follower_ids = get_follower_ids(user_id, count=count)
    return get_users_for_user_ids(follower_ids)

def get_timeline(user_id, start=None, limit=40):
    return _get_line(TIMELINE, user_id, start, limit)

def get_userline(user_id, start=None, limit=40):
    return _get_line(USERLINE, user_id, start, limit)

def get_tweet(tweet_id):
    try:
        tweet = TWEET.get(str(tweet_id))
    except NotFoundException:
        raise NotFound('Tweet %s not found' % (tweet_id,))
    try:
        return dict(((k, json.loads(v)) for k, v in tweet.iteritems()))
    except IndexError:
        raise NotFound('Tweet %s not found' % (tweet_id,))


# INSERTING APIs

def save_user(user_id, user):
    encoded = dict(((k, json.dumps(v)) for k, v in user.iteritems()))
    USER.insert(str(user_id), encoded)
    if 'username' in user:
        key = user['username'].encode('utf-8')
        USERNAME.insert(key, {'id': str(user_id)})

def save_tweet(tweet_id, user_id, tweet):
    raw_ts = int(time.time() * 1e6)
    ts = _long(raw_ts)
    tweet['_ts'] = raw_ts
    encoded = dict(((k, json.dumps(v)) for k, v in tweet.iteritems()))
    TWEET.insert(str(tweet_id), encoded)
    USERLINE.insert(str(user_id), {ts: str(tweet_id)})
    USERLINE.insert(PUBLIC_USERLINE_KEY, {ts: str(tweet_id)})
    follower_ids = [user_id] + get_follower_ids(user_id)
    for follower_id in follower_ids:
        TIMELINE.insert(str(follower_id), {ts: str(tweet_id)})

def add_friends(from_user, to_users):
    ts = str(int(time.time() * 1e6))
    dct = OrderedDict(((str(user_id), ts) for user_id in to_users))
    FRIENDS.insert(str(from_user), dct)
    for to_user_id in to_users:
        FOLLOWERS.insert(str(to_user_id), {str(from_user): ts})

def remove_friends(from_user, to_users):
    for user_id in to_users:
        FRIENDS.remove(str(from_user), column=str(user_id))
    for to_user_id in to_users:
        FOLLOWERS.remove(str(to_user_id), column=str(to_user_id))