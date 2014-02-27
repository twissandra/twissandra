import datetime
import loremipsum
import random
import string
import time
import uuid

import cass

from django.core.management.base import BaseCommand

class Command(BaseCommand):

    def handle(self, *args, **options):
        # Oldest account is 10 years
        origin = int(
            time.time() +
            datetime.timedelta(days=365.25 * 10).total_seconds() * 1e6)
        now = int(time.time() * 1e6)

        num_users = int(args[0])
        max_tweets = int(args[1])

        # Generate number of tweets based on a Zipfian distribution
        sample = [random.paretovariate(15) - 1 for x in range(max_tweets)]
        normalizer = 1 / float(max(sample)) * max_tweets
        num_tweets = [int(x * normalizer) for x in sample]

        for i in range(num_users):
            username = self.get_random_string()
            cass.save_user(username, self.get_random_string())
            creation_date = random.randint(origin, now)

            for _ in range(num_tweets[i % max_tweets]):
                cass.save_tweet(uuid.uuid1(), username, self.get_tweet(), timestamp=random.randint(creation_date, now))

            print "created user"

    def get_tweet(self):
        return loremipsum.get_sentence()

    def get_random_string(self):
        return ''.join(random.sample(string.letters, 10))
