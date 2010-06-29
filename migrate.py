#!/usr/bin/env python

#
# A quick migration from id-based users to uname-based users.
# Please flush and compact when you take down the server,
#  then after bringing down the server to update the code,
#  run this migration before bringing the server back up.
#

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'deps'))

from odict import OrderedDict
import pycassa
import cass

def migrate_users(client,ks):
    print "  * Migrating Users"
    USER = pycassa.ColumnFamily(client, ks, 'User', dict_class=OrderedDict)
    uid_to_uname = dict()
    for old_row in USER.get_range():
        old_row_cols = old_row[1]
        if old_row_cols.has_key('id'):
            # Map the id to the uname for later use
            uid_to_uname[old_row_cols['id']] = old_row_cols['username']
            # Get rid of id and username
            del old_row_cols['id']
            uname = old_row_cols['username']
            del old_row_cols['username']
            # Delete old, insert new with uname key and all the remaining args
            USER.remove(old_row[0])
            USER.insert(uname, old_row_cols)
            # This print will probably spam you to death. Please silence it.
            print '    * User ' + uname + ' has been migrated.'

    # Now that we have a uid_to_uname mapping, it's time to
    #   change every other place where uid shows up.
    print "  * Migrating uid to uname globally"
    print "    * Migrating Friends"
    FRIENDS = pycassa.ColumnFamily(client, ks, 'Friends', dict_class=OrderedDict)
    for old_row in FRIENDS.get_range():
        if uid_to_uname.has_key(old_row[0]):
            friends = old_row[1]
            for friend in friends.keys():
                friends[uid_to_uname[friend]] = friends[friend]
                del friends[friend]
            FRIENDS.remove(old_row[0])
            FRIENDS.insert(uid_to_uname[old_row[0]],friends)
            # This print will probably spam you to death. Please silence it.
            print '      * Friends of ' + uid_to_uname[old_row[0]] + ' have been migrated.'

    print "    * Migrating Followers"
    FOLLOWERS = pycassa.ColumnFamily(client, ks, 'Followers', dict_class=OrderedDict)
    for old_row in FOLLOWERS.get_range():
        if uid_to_uname.has_key(old_row[0]):
            followers = old_row[1]
            for follower in followers.keys():
                followers[uid_to_uname[follower]] = followers[follower]
                del followers[follower]
            FOLLOWERS.remove(old_row[0])
            FOLLOWERS.insert(uid_to_uname[old_row[0]],followers)
            # This print will probably spam you to death. Please silence it.
            print '      * Followers of ' + uid_to_uname[old_row[0]] + ' have been migrated.'


    print "    * Migrating Tweets"
    TWEET = pycassa.ColumnFamily(client, ks, 'Tweet', dict_class=OrderedDict)
    for old_row in TWEET.get_range():
        old_row_cols = old_row[1]
        if old_row_cols.has_key('id'):
          del old_row_cols['id']
          del old_row_cols['_ts']
          old_row_cols['uname'] = uid_to_uname[old_row_cols['user_id']]
          del old_row_cols['user_id']
          TWEET.remove(old_row[0])
          TWEET.insert(old_row[0],old_row_cols)
          # This print will probably spam you to death. Please silence it.
          print '      * Tweet ' + old_row[0] + ' has been migrated.'

    print "    * Migrating Timeline"
    TIMELINE  = pycassa.ColumnFamily(client, ks, 'Timeline', dict_class=OrderedDict)
    for old_row in TIMELINE.get_range():
        old_row_cols = old_row[1]
        if uid_to_uname.has_key(old_row[0]):
            TIMELINE.remove(old_row[0])
            TIMELINE.insert(uid_to_uname[old_row[0]],old_row_cols)

    print "    * Migrating Userline"
    USERLINE  = pycassa.ColumnFamily(client, ks, 'Userline', dict_class=OrderedDict)
    for old_row in USERLINE.get_range():
        old_row_cols = old_row[1]
        if uid_to_uname.has_key(old_row[0]):
            USERLINE.remove(old_row[0])
            USERLINE.insert(uid_to_uname[old_row[0]],old_row_cols)
   

if __name__ == '__main__':
    print "  * Connecting to the database in keyspace 'Twissandra'"
    client = pycassa.connect_thread_local(framed_transport=True)
    ks = "Twissandra"
    migrate_users(client,ks)
