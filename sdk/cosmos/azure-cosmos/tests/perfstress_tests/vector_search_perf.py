import gc
import json
import sys
import tracemalloc
from azure.cosmos.aio import CosmosClient
from devtools_testutils.perfstress_tests import PerfStressTest # type: ignore
import random
from viztracer import VizTracer
from memory_profiler import profile
from guppy import hpy
import logging 


class VectorSearchTest(PerfStressTest):

    def __init__(self, arguments):
        super().__init__(arguments)

        # Auth configuration
        
        URL = ""
        KEY = ""
        self.logging = False
        self.latency = True 
        self.normal = False

        self.client = CosmosClient(URL, credential=KEY)        
        if (self.logging):
            # Create clients
            #Create a logger for the 'azure' SDK
            logger = logging.getLogger('azure')
            logger.setLevel(logging.DEBUG)

            # Configure a file output
            handler = logging.FileHandler(filename="python-diagnostics")
            logger.addHandler(handler)
            self.client = CosmosClient(URL, credential=KEY, logger=logger, enable_diagnostics_logging=True)
        
        database = self.client.get_database_client('vector search test')
        self.container = database.get_container_client('test3')
        self.queries = []
        self.top_stats = []
        self.h = hpy()
        """ self.tracer = VizTracer()
        self.tracer.start() """

    async def global_setup(self):
        """The global setup is run only once.

        Use this for any setup that can be reused multiple times by all test instances.
        """
        if not self.normal:
            for i in range(100):
                embedding = [random.uniform(-1, 1) for _ in range(128)]
                test_query ='SELECT TOP 100 c.authors AS RepresentedData FROM c ORDER BY VectorDistance(c.Embedding, ' + str(embedding) + ')'
                self.queries.append(test_query)
        else:
            self.queries.append("SELECT TOP 10000 c.authors AS RepresentedData FROM c ORDER BY c.authors DESC")

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
            for i in range(100):
                await self.runQuery()
            current, peak = tracemalloc.get_traced_memory()
            print(before)
            print("Memory usage after queries is: ", current/(1024*1024), "MB")
            gc.collect()
            current, peak = tracemalloc.get_traced_memory()
            print("Memory usage after garbage collection is: ", current/(1024*1024), "MB")
            


    async def runQuery(self):
        results = self.container.query_items(query=self.queries[0])
        item_list = []
        async for item in results:
            item_list.append(item)



