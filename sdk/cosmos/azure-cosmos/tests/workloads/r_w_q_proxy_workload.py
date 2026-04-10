# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.
from azure.core.pipeline.transport._aiohttp import AioHttpTransport

from workload_utils import *
from workload_configs import *
from perf_config import get_perf_config
from perf_stats import Stats
from perf_reporter import PerfReporter

from azure.cosmos.aio import CosmosClient as AsyncClient
import asyncio

async def run_workload(client_id, client_logger):
    stats = Stats()
    perf_config = get_perf_config()
    reporter = None
    if perf_config["enabled"] and perf_config["results_endpoint"]:
        reporter = PerfReporter(stats, perf_config)
        reporter.start()

    try:
        session = create_custom_session()
        async with AsyncClient(COSMOS_URI,
                               COSMOS_CREDENTIAL,
                               multiple_write_locations=USE_MULTIPLE_WRITABLE_LOCATIONS,
                               preferred_locations=PREFERRED_LOCATIONS,
                               transport=AioHttpTransport(session=session, session_owner=False),
                               excluded_locations=CLIENT_EXCLUDED_LOCATIONS,
                               enable_diagnostics_logging=True,
                               logger=client_logger,
                               user_agent=get_user_agent(client_id)
                               ) as client:
            db = client.get_database_client(COSMOS_DATABASE)
            cont = db.get_container_client(COSMOS_CONTAINER)
            await asyncio.sleep(1)

            while True:
                try:
                    await upsert_item_concurrently(cont, REQUEST_EXCLUDED_LOCATIONS, CONCURRENT_REQUESTS, stats)
                    await read_item_concurrently(cont, REQUEST_EXCLUDED_LOCATIONS, CONCURRENT_REQUESTS, stats)
                    await query_items_concurrently(cont, REQUEST_EXCLUDED_LOCATIONS, CONCURRENT_QUERIES, stats)
                except Exception as e:
                    client_logger.info("Exception in application layer")
                    client_logger.error(e)
    finally:
        if reporter:
            reporter.stop()


if __name__ == "__main__":
    file_name = os.path.basename(__file__)
    prefix, logger = create_logger(file_name)
    asyncio.run(run_workload(prefix, logger))
