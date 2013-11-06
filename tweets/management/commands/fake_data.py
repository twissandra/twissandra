import datetime
import loremipsum
import numpy
import random
import string
import time
import uuid

import cass

from django.core.management.base import BaseCommand

class Command(BaseCommand):

    def handle(self, *args, **options):
        self.words = filter(lambda x: len(x) <= 4,
                open('/usr/share/dict/words', 'r'))

        # Oldest account is 10 years
        origin = int(time.time() + datetime.timedelta(days=365.25 * 10)
                .total_seconds() * 1e6)
        now = int(time.time() * 1e6)

        num_users = int(args[0])
        max_tweets = int(args[1])

        # Generate number of tweets based on a Zipfian distribution
        sample = numpy.random.zipf(1.05, max_tweets) - 1
        num_tweets = numpy.around(sample / float(max(sample)) * max_tweets, 0)

        for i in range(num_users):
            username = self.get_username()
            cass.save_user(username, {'password': self.get_password()})
            creation_date = random.randint(origin, now)

            for _ in range(int(num_tweets[i % max_tweets])):
                cass.save_tweet(str(uuid.uuid1()), username, {
                    'username': username,
                    'body': self.get_tweet(),
                }, timestamp=random.randint(creation_date, now))

    def get_username(self):
        return filter(lambda x: x in string.letters + string.digits,
                ''.join(random.sample(self.words, 2)))

    def get_tweet(self):
        return loremipsum.get_sentence()

    def get_password(self):
        return ''.join(random.sample(string.letters, 10))
