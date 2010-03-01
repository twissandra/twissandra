# Twissandra

Twissandra is an example project, created to learn and demonstrate how to use
Cassandra.  Running the project will present a website that has similar
functionality to Twitter.

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

    svn co https://svn.apache.org/repos/asf/incubator/cassandra/trunk cassandra

### Check out the Twissandra source code

    git clone git://github.com/ericflo/twissandra.git

### Install and configure Cassandra

First we need to download Cassandra's dependencies and compile the classfiles:

    cd cassandra
    ant ivy-retrieve
    ant build

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