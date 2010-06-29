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
2. Check out the Twissandra source code
3. Install and configure Cassandra
4. Install Thrift
5. Create a virtual Python environment with Twissandra's dependencies
6. Start up the webserver

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

## Upgrade

If you're running ericflo's Twissandra right now, you'll need to migrate your
data to the new schema layout. Here's the steps:

### Kill the running webserver.
### Swap out the code.
Create a new virtualenv for this code and switch to it, then either link the
requirements or download them again. They're not that big.

    cd twissfork
    virtualenv ENV
    source ENV/bin/activate
    pip install -U -r twissandra/requirements.txt

### Run the migrator. 
NOTE: If you have a large amount of data, consider silencing the output of the 
migrator. If you have a toy dataset, then it will just look pretty.

    python migrate.py

### Compact the database.
This is optional, as cassandra will compact itself soon anyway.

    CASS_0.6/bin/nodetool -h 127.0.0.1 compact

### Restart the webserver 
...and continue playing with Twissandra.

## Schema Layout

In Cassandra, the way that your data is structured is very closely tied to how
how it will be retrieved.  Let's start with the user ColumnFamily. The key is
a username, and the columns are the properties on the user:

    User = {
        'hermes': {
            'password': '****',
            (other properties),
        },
    }

Friends and followers are keyed by the username, and then the columns are the
friend names and follower names, and we store a timestamp as the value because
it's interesting information to have:
    
    Friends = {
        'hermes': {
            # friend id: timestamp of when the friendship was added
            'larry': '1267413962580791',
            'curly': '1267413990076949',
            'moe'  : '1267414008133277',
        },
    }
    
    Followers = {
        'hermes': {
            # friend id: timestamp of when the followership was added
            'larry': '1267413962580791',
            'curly': '1267413990076949',
            'moe'  : '1267414008133277',
        },
    }

Tweets are stored with a tweet id for the key.

    Tweet = {
        '7561a442-24e2-11df-8924-001ff3591711': {
            'uname': 'hermes',
            'body': 'Trying out Twissandra. This is awesome!',
        },
    }

The Timeline and Userline column families keep track of which tweets should
appear, and in what order.  To that effect, the key is the username, the column
name is a timestamp, and the column value is the tweet id:

    Timeline = {
        'hermes': {
            # timestamp of tweet: tweet id
            1267414247561777: '7561a442-24e2-11df-8924-001ff3591711',
            1267414277402340: 'f0c8d718-24e2-11df-8924-001ff3591711',
            1267414305866969: 'f9e6d804-24e2-11df-8924-001ff3591711',
            1267414319522925: '02ccb5ec-24e3-11df-8924-001ff3591711',
        },
    }
    
    Userline = {
        'hermes': {
            # timestamp of tweet: tweet id
            1267414247561777: '7561a442-24e2-11df-8924-001ff3591711',
            1267414277402340: 'f0c8d718-24e2-11df-8924-001ff3591711',
            1267414305866969: 'f9e6d804-24e2-11df-8924-001ff3591711',
            1267414319522925: '02ccb5ec-24e3-11df-8924-001ff3591711',
        },
    }
