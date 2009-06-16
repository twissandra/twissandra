import sys

from pprint import pprint

from utils import with_thrift, get_user_id, get_friends, get_followers
from utils import get_timeline, get_tweets

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
    pprint(timeline)
    print '--------------------'
    print "TWEETS:"
    pprint(tweets)
    print '--------------------'
    print '%s friends and %s followers' % (len(friends), len(followers))

if __name__ == '__main__':
    main()