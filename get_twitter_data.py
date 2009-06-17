import sys

from pprint import pprint

from database import with_thrift, get_user_id, get_friends, get_followers
from database import get_timeline, get_tweets, get_user

def main():
    username = sys.argv[1]
    
    user_id = get_user_id(username)
    if user_id is None:
        print 'Unable to find data for user %s' % (username,)
        return None
    
    friends = get_friends(user_id)
    followers = get_followers(user_id)
    
    timeline = get_timeline(user_id)
    tweets = get_tweets(user_id)
    
    print "FRIENDS:"
    print friends
    print '--------------------'
    print "FOLLOWERS:"
    print followers
    print '--------------------'
    print "TIMELINE:"
    for tweet in timeline:
        print '%s: %s' % (username.ljust(9), tweet['text'])
    print '--------------------'
    print "TWEETS:"
    for tweet in tweets:
        user = get_user(tweet['user_id'])
        print '%s: %s' % (user['screen_name'].ljust(9), tweet['text'])
    print '--------------------'
    print '%s friends and %s followers' % (len(friends), len(followers))

if __name__ == '__main__':
    main()