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
    'save_tweet', 'add_friends', 'remove_friends']

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
    return map(long, friends.values())

def _get_userline_or_timeline(cf, user_id, start, limit):
    start = _long(start) if start else ''
    try:
        timeline = cf.get(str(user_id), column_start=start, column_count=limit)
    except NotFoundException:
        return []
    tweets = TWEET.multiget(map(str, timeline.values()))
    decoded = []
    for tweet in tweets.values():
        decoded.append(
            dict(((k, json.loads(v)) for k, v in tweet.iteritems()))
        )
    return decoded


# QUERYING APIs

def get_user_by_id(user_id):
    user = USER.get(str(user_id))
    return dict(((k, json.loads(v)) for k, v in user.iteritems()))

def get_user_by_username(username):
    record = USERNAME.get(username.encode('utf-8'))
    if 'id' not in record:
        raise NotFound('Username %s not found' % (username,))
    return get_user_by_id(record['id'])

def get_friend_ids(user_id, count=5000):
    return _get_friend_or_follower_ids(FRIENDS, user_id, count)

def get_follower_ids(user_id, count=5000):
    return _get_friend_or_follower_ids(FOLLOWERS, user_id, count)

def get_users_for_user_ids(user_ids):
    users = USER.multiget(map(str, user_ids))
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
    return _get_userline_or_timeline(TIMELINE, user_id, start, limit)

def get_userline(user_id, start=None, limit=40):
    return _get_userline_or_timeline(USERLINE, user_id, start, limit)

def get_tweet(tweet_id):
    tweet = TWEET.get(str(tweet_id))
    try:
        return dict(((k, json.loads(v)) for k, v in tweet.iteritems()))
    except IndexError:
        raise NotFound('Tweet %s not found' % (tweet_id,))


# INSERTING APIs

def save_user(user_id, user):
    encoded = dict(((k, json.dumps(v)) for k, v in user.iteritems()))
    USER.insert(str(user_id), encoded)
    if 'screen_name' in user:
        key = user['screen_name'].encode('utf-8')
        USERNAME.insert(key, {'id': str(user_id)})

def save_tweet(tweet_id, user_id, tweet):
    ts = _long(int(time.time() * 1e6))
    encoded = dict(((k, json.dumps(v)) for k, v in tweet.iteritems()))
    TWEET.insert(str(tweet_id), encoded)
    USERLINE.insert(str(tweet_id), {ts: str(tweet_id)})
    follower_ids = [user_id] + get_follower_ids(user_id)
    for follower_id in follower_ids:
        TIMELINE.insert(str(follower_id), {ts: str(tweet_id)})

def add_friends(from_user, to_users):
    ts = _long(int(time.time() * 1e6))
    dct = OrderedDict(((ts, str(user_id)) for user_id in to_users))
    FRIENDS.insert(str(from_user), dct)
    for to_user_id in to_users:
        FOLLOWERS.insert(str(to_user_id), {ts: str(from_user)})

def remove_friends(from_user, to_users):
    for user_id in to_users:
        FRIENDS.remove(str(from_user), column=_long(user_id))
    for to_user_id in to_users:
        FOLLOWERS.remove(str(to_user_id), column=_long(from_user))