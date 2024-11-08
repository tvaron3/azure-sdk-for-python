# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.
import random
import time
import unittest
import uuid


import azure.cosmos.cosmos_client as cosmos_client
import test_config
from azure.cosmos import DatabaseProxy, PartitionKey
from azure.cosmos._change_feed.feed_range_internal import FeedRangeInternalEpk
from azure.cosmos._session_token_helpers import is_compound_session_token, parse_session_token
from azure.cosmos.http_constants import HttpHeaders


def create_item(hpk):
    if hpk:
        item = {
            'id': 'item' + str(uuid.uuid4()),
            'name': 'sample',
            'state': 'CA',
            'city': 'LA' + str(random.randint(1, 10)),
            'zipcode': '90001'
        }
    else:
        item = {
            'id': 'item' + str(uuid.uuid4()),
            'name': 'sample',
            'pk': 'A' + str(random.randint(1, 10))
        }
    return item


class TestLatestSessionToken(unittest.TestCase):
    """Test for session token helpers"""

    created_db: DatabaseProxy = None
    client: cosmos_client.CosmosClient = None
    host = test_config.TestConfig.host
    masterKey = test_config.TestConfig.masterKey
    configs = test_config.TestConfig
    TEST_DATABASE_ID = configs.TEST_DATABASE_ID

    @classmethod
    def setUpClass(cls):
        cls.client = cosmos_client.CosmosClient(cls.host, cls.masterKey)
        cls.database = cls.client.get_database_client(cls.TEST_DATABASE_ID)


    def test_latest_session_token_from_logical_pk(self):
        container = self.database.create_container("test_updated_session_token_from_logical_pk" + str(uuid.uuid4()),
                                                   PartitionKey(path="/pk"),
                                                   offer_throughput=400)
        feed_ranges_and_session_tokens = []
        previous_session_token = ""
        target_pk = 'A1'
        target_feed_range = container.feed_range_from_partition_key(target_pk)
        target_session_token, previous_session_token = self.create_items_logical_pk(container, target_feed_range,
                                                                                    previous_session_token,
                                                                                    feed_ranges_and_session_tokens)
        session_token = container.get_latest_session_token(feed_ranges_and_session_tokens, target_feed_range)

        assert session_token == target_session_token
        feed_ranges_and_session_tokens.append((target_feed_range, session_token))

        self.trigger_split(container, 11000)

        target_session_token, _ = self.create_items_logical_pk(container, target_feed_range, session_token,
                                                               feed_ranges_and_session_tokens)
        target_feed_range = container.feed_range_from_partition_key(target_pk)
        session_token = container.get_latest_session_token(feed_ranges_and_session_tokens, target_feed_range)

        assert session_token == target_session_token
        self.database.delete_container(container.id)

    def test_latest_session_token_from_physical_pk(self):
        container = self.database.create_container("test_updated_session_token_from_physical_pk" + str(uuid.uuid4()),
                                                   PartitionKey(path="/pk"),
                                                    offer_throughput=400)
        feed_ranges_and_session_tokens = []
        previous_session_token = ""
        pk_feed_range = container.feed_range_from_partition_key('A1')
        target_session_token, target_feed_range, previous_session_token = self.create_items_physical_pk(container, pk_feed_range,
                                                                                                   previous_session_token,
                                                                                                   feed_ranges_and_session_tokens)

        session_token = container.get_latest_session_token(feed_ranges_and_session_tokens, target_feed_range)
        assert session_token == target_session_token

        self.trigger_split(container, 11000)

        _, target_feed_range, previous_session_token = self.create_items_physical_pk(container, pk_feed_range,
                                                                                session_token,
                                                                                feed_ranges_and_session_tokens)

        session_token = container.get_latest_session_token(feed_ranges_and_session_tokens, target_feed_range)
        assert is_compound_session_token(session_token)
        session_tokens = session_token.split(",")
        assert len(session_tokens) == 2
        pk_range_id1, session_token1 = parse_session_token(session_tokens[0])
        pk_range_id2, session_token2 = parse_session_token(session_tokens[1])
        pk_range_ids = [pk_range_id1, pk_range_id2]

        assert 320 == (session_token1.global_lsn + session_token2.global_lsn)
        assert '1' in pk_range_ids
        assert '2' in pk_range_ids
        self.database.delete_container(container.id)

    def test_latest_session_token_hpk(self):
        container = self.database.create_container("test_updated_session_token_hpk" + str(uuid.uuid4()),
                                                   PartitionKey(path=["/state", "/city", "/zipcode"], kind="MultiHash"),
                                                   offer_throughput=400)
        feed_ranges_and_session_tokens = []
        previous_session_token = ""
        pk = ['CA', 'LA1', '90001']
        pk_feed_range = container.feed_range_from_partition_key(pk)
        target_session_token, target_feed_range, previous_session_token = self.create_items_physical_pk(container,
                                                                                                        pk_feed_range,
                                                                                                        previous_session_token,
                                                                                                        feed_ranges_and_session_tokens,
                                                                                                        True)

        session_token = container.get_latest_session_token(feed_ranges_and_session_tokens, target_feed_range)
        assert session_token == target_session_token
        self.database.delete_container(container.id)


    def test_latest_session_token_logical_hpk(self):
        self.createDocsForFTS()
        # container = self.database.create_container("test_updated_session_token_from_logical_hpk" + str(uuid.uuid4()),
        #                                            PartitionKey(path=["/state", "/city", "/zipcode"], kind="MultiHash"),
        #                                            offer_throughput=400)
        # feed_ranges_and_session_tokens = []
        # previous_session_token = ""
        # target_pk = ['CA', 'LA1', '90001']
        # target_feed_range = container.feed_range_from_partition_key(target_pk)
        # target_session_token, previous_session_token = self.create_items_logical_pk(container, target_feed_range,
        #                                                                             previous_session_token,
        #                                                                             feed_ranges_and_session_tokens,
        #                                                                             True)
        # session_token = container.get_latest_session_token(feed_ranges_and_session_tokens, target_feed_range)
        #
        # assert session_token == target_session_token
        # self.database.delete_container(container.id)

    def createDocsForFTS(self):

        #{'id': 'item0', 'pk': '1', 'text': 'Good morning!', 'embedding': [-0.008334724, -0.05993167, -0.0903545, -0.04791922, -0.01825805, -0.053011455, 0.120733805, 0.017714009, 0.07346743, 0.11559805, 0.03262076, 0.074512, 0.015864266, 0.01981401, 0.007850527, 0.076296456, -0.08539284, 0.016593281, -0.05423011, 0.07520837, 0.074250855, 0.056754466, -0.022098986, 0.03155444, 0.04334927, 0.024655985, -0.02109795, 0.044023883, -0.027550288, -0.11350893, -0.022806242, 0.08608921, 0.009221513, 0.06659074, 0.09087678, 0.024830079, 0.0075513036, 0.036472578, 0.015418151, 0.060497474, 0.010940685, -0.059365865, 0.043566886, 0.00427073, -0.023546139, 0.030357545, -0.03403527, 0.1182965, 0.1115939, -0.018954424, 0.0032452107, 0.10297628, 0.15328929, -0.016952349, -0.04530782, 0.04674409, -8.351895e-05, -0.19376601, -0.025091218, -0.03664667, -0.011860116, -0.10454312, -0.13109237, -0.049268447, 0.17557324, 0.044872586, 0.046787616, 0.15337633, -0.019509347, 0.0077743605, 0.04556896, -0.08413066, -0.028681897, 0.1209079, 0.1357929, -0.09314, 0.12534729, -0.065546185, 0.12212656, 0.04892026, 0.07394619, -0.08134516, -0.004493787, 0.08138869, 0.028573086, 0.12290998, -0.16477945, -0.29839617, -0.08090993, 0.12256179, 0.16591106, -0.08173688, -0.034383457, -0.1076768, -0.043022845, -0.07655759, 0.2021225, 0.03923631, 0.07703635, -0.08587159, 0.06498038, -0.08330371, 0.16486649, -0.14040637, 0.02070624, -0.069855, 0.052880887, 0.016136287, 0.00024294876, -0.19968519, 0.06933272, 0.013241983, 0.0004002109, 0.14998151, 0.07516485, 0.18610589, -0.07895138, -0.108982496, -0.03494926, -0.027637335, -0.032925423, -0.009509855, 0.1182965, -0.075513035, -0.08665501, 0.019629037, 0.2583547, 0.00983084]},
        indexing_policy = {
            "automatic": True,
            "includedPaths": [
                {
                    "path": "/*"
                }
            ],
            "excludedPaths": [
                {
                    "path": "/_etag/?",
                },
                {
                    "path": "/embedding/*"
                }
            ],
            "vectorIndexes": [
                {
                    "path": "/embedding",
                    "type": "quantizedFlat"
                }
            ],
            "fullTextIndexes": [
                {"path": "/text"}
            ]
        }
        full_text_policy = {
            "defaultLanguage": "en-US",
            "fullTextPaths": [
                {
                    "path": "/text1",
                    "language": "en-US"
                }
            ]
        }

        db = self.client.get_database_client("Test")

        container = db.create_container(
            id="fts",
            partition_key=PartitionKey(path="/pk"),
            offer_throughput=test_config.TestConfig.THROUGHPUT_FOR_1_PARTITION,
            indexing_policy=indexing_policy,
            vector_embedding_policy=test_config.get_vector_embedding_policy(data_type="float32",
                                                                            distance_function="cosine",
                                                                            dimensions=128),
            full_text_policy=full_text_policy)
        assert container is not None
        # for i in range(100):
        #     item = {
        #         'id': 'item' + str(i),
        #         'name': 'sample',
        #         'pk': 'A' + str(random.randint(1, 10))
        #     }


    @staticmethod
    def trigger_split(container, throughput):
        print("Triggering a split in session token helpers")
        container.replace_throughput(throughput)
        print("changed offer to 11k")
        print("--------------------------------")
        print("Waiting for split to complete")
        start_time = time.time()

        while True:
            offer = container.get_throughput()
            if offer.properties['content'].get('isOfferReplacePending', False):
                if time.time() - start_time > 60 * 25:  # timeout test at 25 minutes
                    unittest.skip("Partition split didn't complete in time.")
                else:
                    print("Waiting for split to complete")
                    time.sleep(60)
            else:
                break
        print("Split in session token helpers has completed")

    @staticmethod
    def create_items_logical_pk(container, target_pk_range, previous_session_token, feed_ranges_and_session_tokens, hpk=False):
        target_session_token = ""
        for i in range(100):
            item = create_item(hpk)
            response = container.create_item(item, session_token=previous_session_token)
            session_token = response.get_response_headers()[HttpHeaders.SessionToken]
            pk = item['pk'] if not hpk else [item['state'], item['city'], item['zipcode']]
            pk_range = container.feed_range_from_partition_key(pk)
            pk_feed_range_epk = FeedRangeInternalEpk.from_json(pk_range)
            target_feed_range_epk = FeedRangeInternalEpk.from_json(target_pk_range)
            if (pk_feed_range_epk.get_normalized_range() ==
                    target_feed_range_epk.get_normalized_range()):
                target_session_token = session_token
            previous_session_token = session_token
            feed_ranges_and_session_tokens.append((pk_range,
                                               session_token))
        return target_session_token, previous_session_token

    @staticmethod
    def create_items_physical_pk(container, pk_feed_range, previous_session_token, feed_ranges_and_session_tokens, hpk=False):
        target_session_token = ""
        container_feed_ranges = list(container.read_feed_ranges())
        target_feed_range = None
        for feed_range in container_feed_ranges:
            if container.is_feed_range_subset(feed_range, pk_feed_range):
                target_feed_range = feed_range
                break

        for i in range(100):
            item = create_item(hpk)
            response = container.create_item(item, session_token=previous_session_token)
            session_token = response.get_response_headers()[HttpHeaders.SessionToken]
            if hpk:
                pk = [item['state'], item['city'], item['zipcode']]
                curr_feed_range = container.feed_range_from_partition_key(pk)
            else:
                curr_feed_range = container.feed_range_from_partition_key(item['pk'])
            if container.is_feed_range_subset(target_feed_range, curr_feed_range):
                target_session_token = session_token
            previous_session_token = session_token
            feed_ranges_and_session_tokens.append((curr_feed_range, session_token))

        return target_session_token, target_feed_range, previous_session_token

if __name__ == '__main__':
    unittest.main()
