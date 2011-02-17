import pycassa
from pycassa.system_manager import *

from django.core.management.base import NoArgsCommand

class Command(NoArgsCommand):

    def handle_noargs(self, **options):
        sys = SystemManager()

        # If there is already a Twissandra keyspace, we have to ask the user
        # what they want to do with it.
        if 'Twissandra' in sys.list_keyspaces():
            msg = 'Looks like you already have a Twissandra keyspace.\nDo you '
            msg += 'want to delete it and recreate it? All current data will '
            msg += 'be deleted! (y/n): '
            resp = raw_input(msg)
            if not resp or resp[0] != 'y':
                print "Ok, then we're done here."
                return
            sys.drop_keyspace('Twissandra')

        sys.create_keyspace('Twissandra', replication_factor=1)
        sys.create_column_family('Twissandra', 'User', comparator_type=UTF8_TYPE)
        sys.create_column_family('Twissandra', 'Friends', comparator_type=BYTES_TYPE)
        sys.create_column_family('Twissandra', 'Followers', comparator_type=BYTES_TYPE)
        sys.create_column_family('Twissandra', 'Tweet', comparator_type=UTF8_TYPE)
        sys.create_column_family('Twissandra', 'Timeline', comparator_type=LONG_TYPE)
        sys.create_column_family('Twissandra', 'Userline', comparator_type=LONG_TYPE)

        print 'All done!'
