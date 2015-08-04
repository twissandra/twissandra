# Twissandra

Twissandra is an example project, created to learn and demonstrate how to use
Cassandra.  Running the project will present a website that has similar
functionality to Twitter.

Most of the magic happens in twissandra/cass.py, so check that out.

## Installation

Installing Twissandra is fairly straightforward.  Really it just involves
checking out Cassandra and Twissandra, doing a little configuration, and
then starting it up.  Here's a roadmap of the steps we're going to take to
install the project:

1. Check out the Twissandra source code
2. Install and configure Cassandra
3. Create a virtual Python environment with Twissandra's dependencies
4. Start up the webserver

### Check out the Twissandra source code

    git clone git://github.com/twissandra/twissandra.git

### Install and configure Cassandra

Follow the instructions for [installing and setting up Cassandra](http://wiki.apache.org/cassandra/GettingStarted).
Note, Twissandra requires **at-least Cassandra 2.0** or later.

And then make sure Cassandra is running:

    bin/cassandra -f

### Create a virtual Python environment with Twissandra's dependencies

First, make sure to have virtualenv installed.  If it isn't installed already,
this should do the trick:

    sudo easy_install -U virtualenv

Now let's create a new virtual environment, and begin using it:

    virtualenv twiss
    source twiss/bin/activate

We should install pip, so that we can more easily install Twissandra's
dependencies into our new virtual environment:

    easy_install -U pip

Now let's install all of the dependencies:

    pip install -U -r twissandra/requirements.txt

Now that we've got all of our dependencies installed, we're ready to start up
the server.

### Create the schema

Make sure you're in the Twissandra checkout, and then run the sync_cassandra
command to create the proper keyspace in Cassandra:

    python manage.py sync_cassandra

### Start up the webserver

This is the fun part! We're done setting everything up, we just need to run it:

    python manage.py runserver

Now go to http://127.0.0.1:8000/ and you can play with Twissandra!

## Schema Layout

In Cassandra, the way that your data is structured is very closely tied to how
how it will be retrieved.  Let's start with the 'users' table. The key is
the username, and the remaining columns are properties on the user:

    CREATE TABLE users (
        username text PRIMARY KEY,
        password text
    )

The 'friends' and 'followers' tables have a compound primary key. The first
component, the "partition key", controls how the data is spread around the
cluster.  The second component, the "clustering key", controls how the data
is sorted on disk.  In this case, the sort order isn't very interesting,
but what's important is that all friends and all followers of a user will be
stored contiguously on disk, making a query to lookup all friends or followers
of a user very efficient.

    CREATE TABLE friends (
        username text,
        friend text,
        since timestamp,
        PRIMARY KEY (username, friend)
    )

    CREATE TABLE followers (
        username text,
        follower text,
        since timestamp,
        PRIMARY KEY (username, follower)
    )

Tweets are stored with a UUID for the key.

    CREATE TABLE tweets (
        tweet_id uuid PRIMARY KEY,
        username text,
        body text
    )

The 'timeline' and 'userline' tables keep track of what tweets were
made and in what order.  To acheive this, we use a TimeUUID for the
clustering key, resulting in tweets being stored in chronological
order.  The "WITH CLUSERING ORDER" option just means that the
tweets will be stored in reverse chronological order (newest first),
which is slightly more efficient for the queries we'll be performing.

    CREATE TABLE userline (
        username text,
        time timeuuid,
        tweet_id uuid,
        PRIMARY KEY (username, time)
    ) WITH CLUSTERING ORDER BY (time DESC)

    CREATE TABLE timeline (
        username text,
        time timeuuid,
        tweet_id uuid,
        PRIMARY KEY (username, time)
    ) WITH CLUSTERING ORDER BY (time DESC)


## Fake data generation

For testing purposes, you can populate the database with some fake tweets.

    python manage.py fake_data <num_users> <max_tweets>

`num_users` is the total number of users to generate and `max_tweets` is the
maximum number of tweets per user. The number of tweets per user is determined
by the Pareto distribution so the number of tweets actually generated will vary
between runs.
