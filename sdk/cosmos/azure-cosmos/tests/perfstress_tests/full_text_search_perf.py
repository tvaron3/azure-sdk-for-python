import gc
import tracemalloc
from azure.cosmos.aio import CosmosClient
from devtools_testutils.perfstress_tests import PerfStressTest # type: ignore
import random
from guppy import hpy
import logging
from test_config import TestConfig


class FullTextSearchTest(PerfStressTest):

    def __init__(self, arguments):
        super().__init__(arguments)

        # Auth configuration

        self.logging = False
        self.latency = True
        self.ru = False
        self.queryIndex = 1    # 0 for full text search, 1 for full text rank, and 2 for hybrid search
        self.num_memory_queries = 100
        top = 1000
        useTop = True
        top_str = "TOP " + str(top) + " " if useTop else ""

        self.client = CosmosClient(TestConfig.host, credential=TestConfig.credential)
        if (self.logging):
            # Create clients
            #Create a logger for the 'azure' SDK
            logger = logging.getLogger('azure')
            logger.setLevel(logging.DEBUG)

            # Configure a file output
            handler = logging.FileHandler(filename="python-diagnostics")
            logger.addHandler(handler)
            self.client = CosmosClient(TestConfig.host, credential=TestConfig.credential, logger=logger, enable_diagnostics_logging=True)

        database = self.client.get_database_client('perf-tests-sdks')
        self.container = database.get_container_client('fts')
        embedding = [random.uniform(-1, 1) for _ in range(128)]
        self.queries = ["SELECT " + top_str + "c.id AS Text FROM c WHERE FullTextContains(c.abstract, 'shoulder')",
                        "SELECT " + top_str + "c.id AS Text FROM c Order By Rank FullTextScore(c.text, ['may', 'music'])",
                        "SELECT " + top_str + "c.id AS text FROM c ORDER BY RANK RRF(FullTextScore(c.abstract, ['may', 'music']), VectorDistance(c.vector," + str(embedding) + ")) "]
        self.top_stats = []
        self.h = hpy()
        """ self.tracer = VizTracer()
        self.tracer.start() """

    async def global_setup(self):
        """The global setup is run only once.

        Use this for any setup that can be reused multiple times by all test instances.
        """
        await super().global_setup()

    async def global_cleanup(self):
        """The global cleanup is run only once.

        Use this to cleanup any resources created in
        setup.
        """
        await super().global_cleanup()

    async def close(self):
        """This is run after cleanup.

        Use this to close any open handles or clients.
        """
        """ self.tracer.stop()
        self.tracer.save() """
        await self.client.close()
        for stat in self.top_stats:
            print(stat)
        await super().close()

    def run_sync(self):
        """The synchronous perf test.

        Try to keep this minimal and focused. Using only a single client API.
        Avoid putting any ancillary logic (e.g. generating UUIDs), and put this in the setup/init instead
        so that we're only measuring the client API call.
        """
        pass

    async def run_async(self):
        """The asynchronous perf test.

        Try to keep this minimal and focused. Using only a single client API.
        Avoid putting any ancillary logic (e.g. generating UUIDs), and put this in the setup/init instead
        so that we're only measuring the client API call.
        """

        if self.latency:
            await self.runQuery()
        else:
            tracemalloc.start()
            current, peak = tracemalloc.get_traced_memory()
            before = "Memory usage before queries is: " + str(current/(1024*1024)) + "MB"
            for i in range(self.num_memory_queries):
                await self.runQuery()
            current, peak = tracemalloc.get_traced_memory()
            print("Peak memory usage is: ", peak/(1024*1024), "MB")
            print(before)
            print("Memory usage after queries is: ", current/(1024*1024), "MB")
            gc.collect()
            current, peak = tracemalloc.get_traced_memory()
            print("Memory usage after garbage collection is: ", current/(1024*1024), "MB")



    async def runQuery(self):
        results = self.container.query_items(query=self.queries[self.queryIndex])
        item_list = []
        async for item in results:
            item_list.append(item)
            #print(item)



