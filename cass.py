from datetime import datetime
from uuid import uuid1, UUID
import random

from cassandra.cluster import Cluster

__all__ = ['get_user_by_username', 'get_friend_usernames',
           'get_follower_usernames', 'get_users_for_usernames', 'get_friends',
           'get_followers', 'get_timeline', 'get_userline', 'get_tweet', 'save_user',
           'save_tweet', 'add_friends', 'remove_friends', 'DatabaseError',
           'NotFound', 'InvalidDictionary', 'PUBLIC_USERLINE_KEY']

CLUSTER = Cluster(['127.0.0.1'])
SESSION = CLUSTER.connect('twissandra')

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


def _get_line(table, username, start, limit):
    """
    Gets a timeline or a userline given a username, a start, and a limit.
    """
    # First we need to get the raw timeline (in the form of tweet ids)
    query = "SELECT time, tweet_id FROM {table} WHERE username=%s {time_clause} LIMIT %s"

    if not start:
        time_clause = ''
        params = (username, limit)
    else:
        time_clause = 'AND time < %s'
        params = (username, UUID(start), limit)

    query = query.format(table=table, time_clause=time_clause)

    results = SESSION.execute(query, params)
    if not results:
        return [], None

    if len(results) == limit:
        # Find the minimum timestamp from our get (the oldest one), and convert
        # it to a non-floating value.
        oldest_timeuuid = min(row.time for row in results)

        # Present the string version of the oldest_timeuuid for the UI...
        next_timeuuid = oldest_timeuuid.urn[len('urn:uuid:'):]
    else:
        next_timeuuid = None

    # Now we fetch the tweets themselves
    futures = []
    for row in results:
        futures.append(SESSION.execute_async(
            "SELECT * FROM tweets WHERE tweet_id=%s", (row.tweet_id, )))

    tweets = [f.result()[0] for f in futures]
    return (tweets, next_timeuuid)


# QUERYING APIs

def get_user_by_username(username):
    """
    Given a username, this gets the user record.
    """
    rows = SESSION.execute("SELECT * FROM users WHERE username=%s", (username, ))
    if not rows:
        raise NotFound('User %s not found' % (username,))
    else:
        return rows[0]


def get_friend_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people that the user is
    following.
    """
    rows = SESSION.execute(
        "SELECT friend FROM friends WHERE username=%s LIMIT %s",
        (username, count))
    return [row.friend for row in rows]


def get_follower_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people following that user.
    """
    rows = SESSION.execute(
        "SELECT follower FROM followers WHERE username=%s LIMIT %s",
        (username, count))
    return [row['follower'] for row in rows]


def get_users_for_usernames(usernames):
    """
    Given a list of usernames, this gets the associated user object for each
    one.
    """
    futures = []
    for user in usernames:
        future = SESSION.execute_async("SELECT * FROM users WHERE username=%s", (user, ))
        futures.append(future)

    users = []
    for user, future in zip(usernames, futures):
        results = future.result()
        if not results:
            raise NotFound('User %s not found' % (user,))
        users.append(results[0])

    return users


def get_friends(username, count=5000):
    """
    Given a username, gets the people that the user is following.
    """
    friend_usernames = get_friend_usernames(username, count=count)
    return get_users_for_usernames(friend_usernames)


def get_followers(username, count=5000):
    """
    Given a username, gets the people following that user.
    """
    follower_usernames = get_follower_usernames(username, count=count)
    return get_users_for_usernames(follower_usernames)


def get_timeline(username, start=None, limit=40):
    """
    Given a username, get their tweet timeline (tweets from people they follow).
    """
    return _get_line("timeline", username, start, limit)


def get_userline(username, start=None, limit=40):
    """
    Given a username, get their userline (their tweets).
    """
    return _get_line("userline", username, start, limit)


def get_tweet(tweet_id):
    """
    Given a tweet id, this gets the entire tweet record.
    """
    results = SESSION.execute("SELECT * FROM tweets WHERE tweet_id=%s", (tweet_id, ))
    if not results:
        raise NotFound('Tweet %s not found' % (tweet_id,))
    else:
        return results[0]


def get_tweets_for_tweet_ids(tweet_ids):
    """
    Given a list of tweet ids, this gets the associated tweet object for each
    one.
    """
    futures = []
    for tweet_id in tweet_ids:
        futures.append(SESSION.execute_async(
            "SELECT * FROM tweets WHERE tweet_id=%s", (tweet_id, )))

    tweets = []
    for tweet_id, future in zip(tweet_id, futures):
        result = future.result()
        if not result:
            raise NotFound('Tweet %s not found' % (tweet_id,))
        else:
            tweets.append(result[0])

    return tweets


# INSERTING APIs

def save_user(username, password):
    """
    Saves the user record.
    """
    SESSION.execute(
        "INSERT INTO users (username, password) VALUES (%s, %s)",
        (username, password))


def _timestamp_to_uuid(time_arg):
    # TODO: once this is in the Cassandra driver, use that
    microseconds = int(time_arg * 1e6)
    timestamp = int(microseconds * 10) + 0x01b21dd213814000L

    time_low = timestamp & 0xffffffffL
    time_mid = (timestamp >> 32L) & 0xffffL
    time_hi_version = (timestamp >> 48L) & 0x0fffL

    rand_bits = random.getrandbits(8 + 8 + 48)
    clock_seq_low = rand_bits & 0xffL
    clock_seq_hi_variant = 0b10000000 | (0b00111111 & ((rand_bits & 0xff00L) >> 8))
    node = (rand_bits & 0xffffffffffff0000L) >> 16
    return UUID(
        fields=(time_low, time_mid, time_hi_version, clock_seq_hi_variant, clock_seq_low, node),
        version=1)


def save_tweet(tweet_id, username, tweet, timestamp=None):
    """
    Saves the tweet record.
    """
    if timestamp:
        now = _timestamp_to_uuid(timestamp)
    else:
        now = uuid1()

    # Insert the tweet, then into the user's timeline, then into the public one
    SESSION.execute(
        "INSERT INTO tweets (tweet_id, username, body) VALUES (%s, %s, %s)",
        (tweet_id, username, tweet))

    SESSION.execute(
        "INSERT INTO userline (username, time, tweet_id) VALUES (%s, %s, %s)",
        (username, now, tweet_id))

    SESSION.execute(
        "INSERT INTO userline (username, time, tweet_id) VALUES (%s, %s, %s)",
        (PUBLIC_USERLINE_KEY, now, tweet_id))

    # Get the user's followers, and insert the tweet into all of their streams
    futures = []
    follower_usernames = [username] + get_follower_usernames(username)
    for follower_username in follower_usernames:
        futures.append(SESSION.execute_async(
            "INSERT INTO timeline (username, time, tweet_id) VALUES (%s, %s, %s)",
            (follower_username, now, tweet_id)))

    for future in futures:
        future.result()


def add_friends(from_username, to_usernames):
    """
    Adds a friendship relationship from one user to some others.
    """
    now = datetime.utcnow()
    futures = []
    for to_user in to_usernames:
        futures.append(SESSION.execute_async(
            "INSERT INTO friends (username, friend, since) VALUES (%s, %s, %s)",
            (from_username, to_user, now)))

        futures.append(SESSION.execute_async(
            "INSERT INTO followers (username, follower, since) VALUES (%s, %s, %s)",
            (to_user, from_username, now)))

    for future in futures:
        future.result()


def remove_friends(from_username, to_usernames):
    """
    Removes a friendship relationship from one user to some others.
    """
    futures = []
    for to_user in to_usernames:
        futures.append(SESSION.execute_async(
            "DELETE FROM friends WHERE username=%s AND friend=%s",
            (from_username, to_user)))

        futures.append(SESSION.execute_async(
            "DELETE FROM followers WHERE username=%s AND follower=%s",
            (to_user, from_username)))

    for future in futures:
        future.result()
