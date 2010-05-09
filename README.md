# Twissandra

Twissandra is an example project, created to learn and demonstrate how to use
Cassandra.  Running the project will present a website that has similar
functionality to Twitter.

You can see a running copy at [http://twissandra.com/](http://twissandra.com/)

Most of the magic happens in twissandra/cass.py, so check that out.

## Installation

Installing Twissandra is fairly straightforward.  Really it just involves
checking out Cassandra and Twissandra, doing a little configuration, and
then starting it up.  Here's a roadmap of the steps we're going to take to
install the project:

1. Check out the latest Cassandra source code
1. Check out the Twissandra source code
1. Install and configure Cassandra
1. Install Thrift
1. Create a virtual Python environment with Twissandra's dependencies
1. Start up the webserver

### Check out the latest Cassandra source code

    curl -O http://download.filehat.com/apache/cassandra/0.6.1/apache-cassandra-0.6.1-bin.tar.gz

### Check out the Twissandra source code

    git clone git://github.com/ericflo/twissandra.git

### Install and configure Cassandra

Now untar cassandra

    tar xvfz apache-cassandra-0.6.1-bin.tar.gz
    cd apache-cassandra-0.6.1

Then we need to create our database directories on disk:

    sudo mkdir -p /var/log/cassandra
    sudo chown -R `whoami` /var/log/cassandra
    sudo mkdir -p /var/lib/cassandra
    sudo chown -R `whoami` /var/lib/cassandra

Now we copy the Cassandra configuration from the Twissandra source tree, and
put it in its proper place in the Cassandra directory structure:

    cp ../twissandra/storage-conf.xml conf/

Finally we can start Cassandra:

    ./bin/cassandra -f

This will run the Cassandra database (configured for Twissandra) in the
foreground, so to continue, we'll need to open a new terminal.

### Install Thrift

Follow the instructions [provided on the Thrift website itself](http://wiki.apache.org/thrift/ThriftInstallation)

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

### Start up the webserver

Make sure you're in the Twissandra checkout, and then start up the server:

    cd twissandra
    python manage.py runserver

Now go to http://127.0.0.1:8000/ and you can play with Twissandra!

## Schema Layout

In Cassandra, the way that your data is structured is very closely tied to how
how it will be retrieved.  Let's start with the user ColumnFamily. The key is
a user id, and the columns are the properties on the user:

    User = {
        'a4a70900-24e1-11df-8924-001ff3591711': {
            'id': 'a4a70900-24e1-11df-8924-001ff3591711',
            'username': 'ericflo',
            'password': '****',
        },
    }

Since some of the URLs on the site actually have the username, we need to be
able to map from the username to the user id:

    Username = {
        'ericflo': {
            'id': 'a4a70900-24e1-11df-8924-001ff3591711',
        },
    }

Friends and followers are keyed by the user id, and then the columns are the
friend user id and follower user ids, and we store a timestamp as the value
because it's interesting information to have:
    
    Friends = {
        'a4a70900-24e1-11df-8924-001ff3591711': {
            # friend id: timestamp of when the friendship was added
            '10cf667c-24e2-11df-8924-001ff3591711': '1267413962580791',
            '343d5db2-24e2-11df-8924-001ff3591711': '1267413990076949',
            '3f22b5f6-24e2-11df-8924-001ff3591711': '1267414008133277',
        },
    }
    
    Followers = {
        'a4a70900-24e1-11df-8924-001ff3591711': {
            # friend id: timestamp of when the followership was added
            '10cf667c-24e2-11df-8924-001ff3591711': '1267413962580791',
            '343d5db2-24e2-11df-8924-001ff3591711': '1267413990076949',
            '3f22b5f6-24e2-11df-8924-001ff3591711': '1267414008133277',
        },
    }

Tweets are stored in a way similar to users:

    Tweet = {
        '7561a442-24e2-11df-8924-001ff3591711': {
            'id': '89da3178-24e2-11df-8924-001ff3591711',
            'user_id': 'a4a70900-24e1-11df-8924-001ff3591711',
            'body': 'Trying out Twissandra. This is awesome!',
            '_ts': '1267414173047880',
        },
    }

The Timeline and Userline column families keep track of which tweets should
appear, and in what order.  To that effect, the key is the user id, the column
name is a timestamp, and the column value is the tweet id:

    Timeline = {
        'a4a70900-24e1-11df-8924-001ff3591711': {
            # timestamp of tweet: tweet id
            1267414247561777: '7561a442-24e2-11df-8924-001ff3591711',
            1267414277402340: 'f0c8d718-24e2-11df-8924-001ff3591711',
            1267414305866969: 'f9e6d804-24e2-11df-8924-001ff3591711',
            1267414319522925: '02ccb5ec-24e3-11df-8924-001ff3591711',
        },
    }
    
    Userline = {
        'a4a70900-24e1-11df-8924-001ff3591711': {
            # timestamp of tweet: tweet id
            1267414247561777: '7561a442-24e2-11df-8924-001ff3591711',
            1267414277402340: 'f0c8d718-24e2-11df-8924-001ff3591711',
            1267414305866969: 'f9e6d804-24e2-11df-8924-001ff3591711',
            1267414319522925: '02ccb5ec-24e3-11df-8924-001ff3591711',
        },
    }
