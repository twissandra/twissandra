import pycassa
from pycassa.cassandra.ttypes import KsDef, CfDef

from django.core.management.base import NoArgsCommand

class Command(NoArgsCommand):

    def handle_noargs(self, **options):
        # First we define all our column families
        column_families = [
            CfDef('Twissandra', 'User', comparator_type='UTF8Type'),
            CfDef('Twissandra', 'Username', comparator_type='BytesType'),
            CfDef('Twissandra', 'Friends', comparator_type='BytesType'),
            CfDef('Twissandra', 'Followers', comparator_type='BytesType'),
            CfDef('Twissandra', 'Tweet', comparator_type='UTF8Type'),
            CfDef('Twissandra', 'Timeline', comparator_type='LongType'),
            CfDef('Twissandra', 'Userline', comparator_type='LongType'),
        ]
        # Now we define our keyspace (with column families inside)
        keyspace = KsDef(
            'Twissandra', # Keyspace Name
            'org.apache.cassandra.locator.SimpleStrategy', # Placement Strat.
            {}, # Options for the Placement Strat.
            1, # Replication factor
            column_families,
        )
        
        client = pycassa.connect('system')

        # If there is already a Twissandra keyspace, we have to ask the user
        # what they want to do with it.
        try:
            client.describe_keyspace('Twissandra')
            # If there were a keyspace, it would have raised an exception.
            msg = 'Looks like you already have a Twissandra keyspace.\nDo you '
            msg += 'want to delete it and recreate it? All current data will '
            msg += 'be deleted! (y/n): '
            resp = raw_input(msg)
            if not resp or resp[0] != 'y':
                print "Ok, then we're done here."
                return
            client.system_drop_keyspace('Twissandra')
        except pycassa.NotFoundException:
            pass
        
        client.system_add_keyspace(keyspace)
        print 'All done!'
