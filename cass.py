import struct
import time

from ordereddict import OrderedDict

import pycassa

from pycassa.cassandra.ttypes import NotFoundException

__all__ = ['get_user_by_username', 'get_friend_unames',
    'get_follower_unames', 'get_users_for_usernames', 'get_friends',
    'get_followers', 'get_timeline', 'get_userline', 'get_tweet', 'save_user',
    'save_tweet', 'add_friends', 'remove_friends', 'DatabaseError',
    'NotFound', 'InvalidDictionary', 'PUBLIC_USERLINE_KEY']

CLIENT = pycassa.connect('Twissandra')

USER = pycassa.ColumnFamily(CLIENT, 'User', dict_class=OrderedDict)
USERNAME = pycassa.ColumnFamily(CLIENT, 'Username', dict_class=OrderedDict)
FRIENDS = pycassa.ColumnFamily(CLIENT, 'Friends', dict_class=OrderedDict)
FOLLOWERS = pycassa.ColumnFamily(CLIENT, 'Followers', dict_class=OrderedDict)
TWEET = pycassa.ColumnFamily(CLIENT, 'Tweet', dict_class=OrderedDict)
TIMELINE = pycassa.ColumnFamily(CLIENT, 'Timeline', dict_class=OrderedDict)
USERLINE = pycassa.ColumnFamily(CLIENT, 'Userline', dict_class=OrderedDict)

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

def _get_friend_or_follower_unames(cf, uname, count):
    """
    Gets the social graph (friends or followers) for a username.
    """
    try:
        friends = cf.get(str(uname), column_count=count)
    except NotFoundException:
        return []
    return friends.keys()

def _get_line(cf, uname, start, limit):
    """
    Gets a timeline or a userline given a username, a start, and a limit.
    """
    # First we need to get the raw timeline (in the form of tweet ids)

    # We get one more tweet than asked for, and if we exceed the limit by doing so,
    #  that tweet's key (timestamp) is returned as the 'next' key for pagination.
    start = long(start) if start else ''
    next = None
    try:
        timeline = cf.get(str(uname), column_start=start, column_count=limit + 1,
            column_reversed=True)
    except NotFoundException:
        return [], next

    if len(timeline) > limit:
        # Find the minimum timestamp from our get (the oldest one), and convert it
        #  to a non-floating value.
        oldest_timestamp = min(timeline.keys())

        # Present the string version of the oldest_timestamp for the UI...
        next = str(oldest_timestamp)

        # And then convert the pylong back to a bitpacked key so we can delete
        #  if from timeline.
        del timeline[oldest_timestamp]

    # Now we do a multiget to get the tweets themselves, comes back in random order
    unordered_tweets = TWEET.multiget(timeline.values())
    # Order the tweets in the order we got back from the timeline
    ordered_tweets   = [unordered_tweets.get(tweet_id) for tweet_id in timeline.values()]

    # We want to get the information about the user who made the tweet
    # First, pull out the list of unique users for our tweets
    usernames = list(set([tweet['uname'] for tweet in ordered_tweets]))
    users = USER.multiget(usernames)
    # Then attach the user record to the tweet
    for tweet in ordered_tweets:
        tweet['user'] = users.get(tweet['uname'])
    return (ordered_tweets, next)


# QUERYING APIs

def get_user_by_username(uname):
    """
    Given a username, this gets the user record.
    """
    try:
        user = USER.get(str(uname))
    except NotFoundException:
        raise NotFound('User %s not found' % (uname,))
    return user

def get_friend_unames(uname, count=5000):
    """
    Given a username, gets the usernames of the people that the user is following.
    """
    return _get_friend_or_follower_unames(FRIENDS, uname, count)

def get_follower_unames(uname, count=5000):
    """
    Given a username, gets the usernames of the people following that user.
    """
    return _get_friend_or_follower_unames(FOLLOWERS, uname, count)

def get_users_for_usernames(unames):
    """
    Given a list of usernames, this gets the associated user object for each
    one.
    """
    try:
        users = USER.multiget(map(str, unames))
    except NotFoundException:
        raise NotFound('Users %s not found' % (unames,))
    return users.values()

def get_friends(uname, count=5000):
    """
    Given a username, gets the people that the user is following.
    """
    friend_unames = get_friend_unames(uname, count=count)
    return get_users_for_user_unames(friend_unames)

def get_followers(uname, count=5000):
    """
    Given a username, gets the people following that user.
    """
    follower_unames = get_follower_unames(uname, count=count)
    return get_users_for_user_unames(follower_unames)

def get_timeline(uname, start=None, limit=40):
    """
    Given a username, get their tweet timeline (tweets from people they follow).
    """
    return _get_line(TIMELINE, uname, start, limit)

def get_userline(uname, start=None, limit=40):
    """
    Given a username, get their userline (their tweets).
    """
    return _get_line(USERLINE, uname, start, limit)

def get_tweet(tweet_id):
    """
    Given a tweet id, this gets the entire tweet record.
    """
    try:
        tweet = TWEET.get(str(tweet_id))
    except NotFoundException:
        raise NotFound('Tweet %s not found' % (tweet_id,))
    return tweet

def get_tweets_for_tweet_ids(tweet_ids):
    """
    Given a list of tweet ids, this gets the associated tweet object for each
    one.
    """
    try:
        tweets = TWEET.multiget(map(str, tweet_ids))
    except NotFoundException:
        raise NotFound('Tweets %s not found' % (tweet_ids,))
    return tweets.values()


# INSERTING APIs

def save_user(uname, user):
    """
    Saves the user record.
    """
    USER.insert(str(uname), user)

def save_tweet(tweet_id, uname, tweet):
    """
    Saves the tweet record.
    """
    # Generate a timestamp for the USER/TIMELINE
    ts = long(time.time() * 1e6)

    # Insert the tweet, then into the user's timeline, then into the public one
    TWEET.insert(str(tweet_id), tweet)
    USERLINE.insert(str(uname), {ts: str(tweet_id)})
    USERLINE.insert(PUBLIC_USERLINE_KEY, {ts: str(tweet_id)})
    # Get the user's followers, and insert the tweet into all of their streams
    follower_unames = [uname] + get_follower_unames(uname)
    for follower_uname in follower_unames:
        TIMELINE.insert(str(follower_uname), {ts: str(tweet_id)})

def add_friends(from_uname, to_unames):
    """
    Adds a friendship relationship from one user to some others.
    """
    ts = str(int(time.time() * 1e6))
    dct = OrderedDict(((str(uname), ts) for uname in to_unames))
    FRIENDS.insert(str(from_uname), dct)
    for to_uname in to_unames:
        FOLLOWERS.insert(str(to_uname), {str(from_uname): ts})

def remove_friends(from_uname, to_unames):
    """
    Removes a friendship relationship from one user to some others.
    """
    for uname in to_unames:
        FRIENDS.remove(str(from_uname), column=str(uname))
    for to_uname in to_unames:
        FOLLOWERS.remove(str(to_uname), column=str(to_uname))
