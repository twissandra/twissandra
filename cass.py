import struct
import time

from odict import OrderedDict

import pycassa

from cassandra.ttypes import NotFoundException

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

# NOTE: Having a single userline key to store all of the public tweets is not
#       scalable.  Currently, Cassandra requires that an entire row (meaning
#       every column under a given key) to be able to fit in memory.  You can
#       imagine that after a while, the entire public timeline would exceed
#       available memory.
#
#       The fix for this is to partition the timeline by time, so we could use
#       a key like !PUBLIC!2010-04-01 to partition it per day.  We could drill
#       down even further into hourly keys, etc.  Since this is a demonstration
#       and that would add quite a bit of extra code, this excercise is left to
#       the reader.
PUBLIC_USERLINE_KEY = '!PUBLIC!'


class DatabaseError(Exception):
    """
    The base error that functions in this module will raise when things go
    wrong.
    """
    pass


class NotFound(DatabaseError):
    pass


class InvalidDictionary(DatabaseError):
    pass


def _long(i):
    """
    Packs a long into the expected sequence of bytes that Cassandra expects.
    """
    return struct.pack('>d', long(i))

def _unlong(b):
    """
    Unpacks Cassandra's byte-representation of longs into their Python long
    equivalents.
    """
    return struct.unpack('>d', b)

def _get_friend_or_follower_ids(cf, user_id, count):
    """
    Gets the social graph (friends or followers) for a user.
    """
    try:
        friends = cf.get(str(user_id), column_count=count)
    except NotFoundException:
        return []
    return friends.keys()

def _get_line(cf, user_id, start, limit):
    """
    Gets a timeline or a userline given a user id, a start, and a limit.
    """
    # First we need to get the raw timeline (in the form of tweet ids)
    start = _long(start) if start else ''
    try:
        timeline = cf.get(str(user_id), column_start=start, column_count=limit,
            column_reversed=True)
    except NotFoundException:
        return []
    # Now we do a multiget to get the tweets themselves
    tweets = TWEET.multiget(timeline.values())
    tweets = dict(((t['id'], t) for t in tweets.values()))
    # Order the tweets in the order we got back from the timeline
    ordered = [tweets.get(tweet_id) for tweet_id in timeline.values()]
    # We want to get the information about the user who made the tweet, so
    # we query for that as well and insert it into the data structure before
    # returning.
    users = get_users_for_user_ids([u['user_id'] for u in ordered])
    users = dict(((u['id'], u) for u in users))
    for tweet in ordered:
        tweet['user'] = users.get(tweet['user_id'])
    return ordered


# QUERYING APIs

def get_user_by_id(user_id):
    """
    Given a user id, this gets the user record.
    """
    try:
        user = USER.get(str(user_id))
    except NotFoundException:
        raise NotFound('User %s not found' % (user_id,))
    return user

def get_user_by_username(username):
    """
    Given a username, this gets the user record.
    """
    # First get the user id
    try:
        record = USERNAME.get(username.encode('utf-8'))
    except NotFoundException:
        raise NotFound('User %s not found' % (username,))
    if 'id' not in record:
        raise NotFound('Username %s not found' % (username,))
    # Now, use the user id to get the whole user record
    return get_user_by_id(record['id'])

def get_friend_ids(user_id, count=5000):
    """
    Given a user id, gets the ids of the people that the user is following.
    """
    return _get_friend_or_follower_ids(FRIENDS, user_id, count)

def get_follower_ids(user_id, count=5000):
    """
    Given a user id, gets the ids of the people following that user.
    """
    return _get_friend_or_follower_ids(FOLLOWERS, user_id, count)

def get_users_for_user_ids(user_ids):
    """
    Given a list of user ids, this gets the associated user object for each
    one.
    """
    try:
        users = USER.multiget(map(str, user_ids))
    except NotFoundException:
        raise NotFound('Users %s not found' % (user_ids,))
    return users.values()

def get_friends(user_id, count=5000):
    """
    Given a user id, gets the people that the user is following.
    """
    friend_ids = get_friend_ids(user_id, count=count)
    return get_users_for_user_ids(friend_ids)

def get_followers(user_id, count=5000):
    """
    Given a user id, gets the people following that user.
    """
    follower_ids = get_follower_ids(user_id, count=count)
    return get_users_for_user_ids(follower_ids)

def get_timeline(user_id, start=None, limit=40):
    """
    Given a user id, get their tweet timeline (tweets from people they follow).
    """
    return _get_line(TIMELINE, user_id, start, limit)

def get_userline(user_id, start=None, limit=40):
    """
    Given a user id, get their userline (their tweets).
    """
    return _get_line(USERLINE, user_id, start, limit)

def get_tweet(tweet_id):
    """
    Given a tweet id, this gets the entire tweet record.
    """
    try:
        tweet = TWEET.get(str(tweet_id))
    except NotFoundException:
        raise NotFound('Tweet %s not found' % (tweet_id,))
    return tweet


# INSERTING APIs

def save_user(user_id, user):
    """
    Saves the user record.
    """
    # First save the user record
    USER.insert(str(user_id), user)
    # Then save the index from username to user id
    if 'username' in user:
        key = user['username'].encode('utf-8')
        USERNAME.insert(key, {'id': str(user_id)})

def save_tweet(tweet_id, user_id, tweet):
    """
    Saves the tweet record.
    """
    # Generate a timestamp, and put it in the tweet record
    raw_ts = int(time.time() * 1e6)
    tweet['_ts'] = str(raw_ts)
    ts = _long(raw_ts)
    # Insert the tweet, then into the user's timeline, then into the public one
    TWEET.insert(str(tweet_id), tweet)
    USERLINE.insert(str(user_id), {ts: str(tweet_id)})
    USERLINE.insert(PUBLIC_USERLINE_KEY, {ts: str(tweet_id)})
    # Get the user's followers, and insert the tweet into all of their streams
    follower_ids = [user_id] + get_follower_ids(user_id)
    for follower_id in follower_ids:
        TIMELINE.insert(str(follower_id), {ts: str(tweet_id)})

def add_friends(from_user, to_users):
    """
    Adds a friendship relationship from one user to some others.
    """
    ts = str(int(time.time() * 1e6))
    dct = OrderedDict(((str(user_id), ts) for user_id in to_users))
    FRIENDS.insert(str(from_user), dct)
    for to_user_id in to_users:
        FOLLOWERS.insert(str(to_user_id), {str(from_user): ts})

def remove_friends(from_user, to_users):
    """
    Removes a friendship relationship from one user to some others.
    """
    for user_id in to_users:
        FRIENDS.remove(str(from_user), column=str(user_id))
    for to_user_id in to_users:
        FOLLOWERS.remove(str(to_user_id), column=str(to_user_id))
