# Copyright Google Inc. 2018
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import time
from google.cloud import pubsub
from faker import Faker
from faker.providers.lorem.en_US import Provider as LoremProvider
from ratelimit import rate_limited


if len(sys.argv) != 4:
    print("""Error: Incorrect number of parameters.

    Usage: python tweet-generator.py <project> <time> <rate>

        - project: ID of your GCP project
        - time: total execution time, in minutes
        - rate: number of tweets generated per minute
""")
    sys.exit()


PROJECT = sys.argv[1]
TOTAL_TIME = int(sys.argv[2])  # in minutes
RATE = int(sys.argv[3])  # in tweets per minute

ONE_MINUTE = 60
TOPIC_NAME = 'tweets'
TWEET_MAX_LENGTH = 140
HASHTAG_MIN_LENGTH = 5
HASHTAG_MAX_LENGTH = 15
HASHTAGS = [
    word for word in LoremProvider.word_list
    if len(word) > HASHTAG_MIN_LENGTH and len(word) <= HASHTAG_MAX_LENGTH
]

publisher = pubsub.PublisherClient()
topic_url = 'projects/{project_id}/topics/{topic}'.format(
    project_id=PROJECT,
    topic=TOPIC_NAME,
)
num_tweets = 0

@rate_limited(RATE, ONE_MINUTE)
def generate_tweet():
    """
    Generates a single random tweet.
    """
    global num_tweets
    fake = Faker()
    hashtag = fake.random_element(HASHTAGS)
    tweet = fake.text(max_nb_chars=TWEET_MAX_LENGTH-HASHTAG_MAX_LENGTH)
    words = tweet.split()
    index = fake.random.randint(0, len(words))
    words.insert(index, '#%s' % hashtag)
    tweet = ' '.join(words)
    publisher.publish(topic_url, tweet.encode('utf-8'))
    num_tweets += 1

now = start_time = time.time()
while now < start_time + TOTAL_TIME * ONE_MINUTE:
    generate_tweet()
    now = time.time()

elapsed_time = time.time() - start_time
print("Elapsed time: %s minutes" % (elapsed_time / 60))
print("Number of generated tweets: %s" % num_tweets)