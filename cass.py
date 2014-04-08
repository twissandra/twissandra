from datetime import datetime
from uuid import uuid1, UUID
import random

from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('twissandra')

# Prepared statements, reuse as much as possible by binding new values
# Insert tweets
prepared_tweets = None
# Add tweets to timelines
prepared_userline = None
prepared_timeline = None
# Addition of friend/follower relationship
prepared_friends = None
prepared_followers = None
# Removing friend / follower relationship
prepared_remove_friends = None
prepared_remove_followers = None
# Adds a new user
prepared_add_user = None
# Retreives tweets
prepared_get_tweets = None
# Retreives usernames
prepared_get_usernames = None
prepared_get_followers = None
prepared_get_friends = None

# NOTE: Having a single userline key to store all of the public tweets is not
#       scalable.  This result in all public tweets being stored in a single
#       partition, which means they must all fit on a single node.
#
#       One fix for this is to partition the timeline by time, so we could use
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
    global prepared_get_tweets
    if prepared_get_tweets is None:
        prepared_get_tweets = session.prepare("""
            SELECT * FROM tweets WHERE tweet_id=?
            """)

    # First we need to get the raw timeline (in the form of tweet ids)
    query = "SELECT time, tweet_id FROM {table} WHERE username=%s {time_clause} LIMIT %s"

    # See if we need to start our page at the beginning or further back
    if not start:
        time_clause = ''
        params = (username, limit)
    else:
        time_clause = 'AND time < %s'
        params = (username, UUID(start), limit)

    query = query.format(table=table, time_clause=time_clause)

    results = session.execute(query, params)
    if not results:
        return [], None

    # If we didn't get to the end, return a starting point for the next page
    if len(results) == limit:
        # Find the oldest ID
        oldest_timeuuid = min(row.time for row in results)

        # Present the string version of the oldest_timeuuid for the UI
        next_timeuuid = oldest_timeuuid.urn[len('urn:uuid:'):]
    else:
        next_timeuuid = None

    # Now we fetch the tweets themselves
    futures = []
    for row in results:
        futures.append(session.execute_async(
            prepared_get_tweets.bind((row.tweet_id, ))))

    tweets = [f.result()[0] for f in futures]
    return (tweets, next_timeuuid)


# QUERYING APIs

def get_user_by_username(username):
    """
    Given a username, this gets the user record.
    """
    global prepared_get_usernames
    if prepared_get_usernames is None:
        prepared_get_usernames = session.prepare("""
            SELECT * FROM users WHERE username=?
            """)

    rows = session.execute(prepared_get_usernames.bind((username, )))
    if not rows:
        raise NotFound('User %s not found' % (username,))
    else:
        return rows[0]


def get_friend_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people that the user is
    following.
    """
    global prepared_get_friends
    if prepared_get_friends is None:
        prepared_get_friends = session.prepare("""
            SELECT friend FROM friends WHERE username=? LIMIT ?
            """)

    rows = session.execute(prepared_get_friends.bind((username, count)))
    return [row.friend for row in rows]


def get_follower_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people following that user.
    """
    global prepared_get_followers
    if prepared_get_followers is None:
        prepared_get_followers = session.prepare("""
            SELECT follower FROM followers WHERE username=? LIMIT ?
            """)

    rows = session.execute(prepared_get_followers.bind((username, count)))
    return [row.follower for row in rows]


def get_users_for_usernames(usernames):
    """
    Given a list of usernames, this gets the associated user object for each
    one.
    """
    global prepared_get_usernames
    if prepared_get_usernames is None:
        prepared_get_usernames = session.prepare("""
            SELECT * FROM users WHERE username=?
            """)

    futures = []
    for user in usernames:
        future = session.execute_async(prepared_get_usernames.bind((user, )))
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
    global prepared_get_tweets
    if prepared_get_tweets is None:
        prepared_get_tweets = session.prepare("""
            SELECT * FROM tweets WHERE tweet_id=?
            """)

    results = session.execute(prepared_get_tweets.bind(tweet_id))
    if not results:
        raise NotFound('Tweet %s not found' % (tweet_id,))
    else:
        return results[0]


def get_tweets_for_tweet_ids(tweet_ids):
    """
    Given a list of tweet ids, this gets the associated tweet object for each
    one.
    """
    global prepared_get_tweets
    if prepared_get_tweets is None:
        prepared_get_tweets = session.prepare("""
            SELECT * FROM tweets WHERE tweet_id=?
            """)

    futures = []
    for tweet_id in tweet_ids:
        futures.append(session.execute_async(prepared_get_tweets.bind(tweet_id)))

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
    global prepared_add_user
    if prepared_add_user is None:
        prepared_add_user = session.prepare("""
            INSERT INTO users (username, password)
            VALUES (?, ?)
            """)

    session.execute(prepared_add_user.bind((username, password)))


def _timestamp_to_uuid(time_arg):
    # TODO: once this is in the python Cassandra driver, use that
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

    global prepared_tweets
    global prepared_userline
    global prepared_timeline

    # Prepare the statements required for adding the tweet into the various timelines
    # Initialise only once, and then re-use by binding new values
    if prepared_tweets is None:
        prepared_tweets = session.prepare("""
            INSERT INTO tweets (tweet_id, username, body)
            VALUES (?, ?, ?)
            """)

    if prepared_userline is None:
        prepared_userline = session.prepare("""
            INSERT INTO userline (username, time, tweet_id)
            VALUES (?, ?, ?)
            """)

    if prepared_timeline is None:
        prepared_timeline = session.prepare("""
            INSERT INTO timeline (username, time, tweet_id)
            VALUES (?, ?, ?)
            """)

    if timestamp is None:
        now = uuid1()
    else:
        now = _timestamp_to_uuid(timestamp)

    # Insert the tweet
    session.execute(prepared_tweets.bind((tweet_id, username, tweet)))
    # Insert tweet into the user's timeline
    session.execute(prepared_userline.bind((username, now, tweet_id)))
    # Insert tweet into the public timeline
    session.execute(prepared_userline.bind((PUBLIC_USERLINE_KEY, now, tweet_id)))

    # Get the user's followers, and insert the tweet into all of their streams
    futures = []
    follower_usernames = [username] + get_follower_usernames(username)
    for follower_username in follower_usernames:
        futures.append(session.execute_async(
            prepared_timeline.bind((follower_username, now, tweet_id))))

    for future in futures:
        future.result()


def add_friends(from_username, to_usernames):
    """
    Adds a friendship relationship from one user to some others.
    """
    global prepared_friends
    global prepared_followers

    if prepared_friends is None:
        prepared_friends = session.prepare("""
            INSERT INTO friends (username, friend, since)
            VALUES (?, ?, ?)
            """)

    if prepared_followers is None:
        prepared_followers = session.prepare("""
            INSERT INTO followers (username, follower, since)
            VALUES (?, ?, ?)
            """)

    now = datetime.utcnow()
    futures = []
    for to_user in to_usernames:
        # Start following user
        futures.append(session.execute_async(
            prepared_friends.bind((from_username, to_user, now))))
        # Add yourself as a follower of the user
        futures.append(session.execute_async(
            prepared_followers.bind((to_user, from_username, now))))

    for future in futures:
        future.result()


def remove_friends(from_username, to_usernames):
    """
    Removes a friendship relationship from one user to some others.
    """
    global prepared_remove_friends
    global prepared_remove_followers

    if prepared_remove_friends is None:
        prepared_remove_friends = session.prepare("""
            DELETE FROM friends WHERE username=? AND friend=?
            """)
    if prepared_remove_followers is None:
        prepared_remove_followers = session.prepare("""
            DELETE FROM followers WHERE username=? AND follower=?
            """)

    futures = []
    for to_user in to_usernames:
        futures.append(session.execute_async(
            prepared_remove_friends.bind((from_username, to_user))))
        futures.append(session.execute_async(
            prepared_remove_followers.bind((to_user, from_username))))

    for future in futures:
        future.result()
