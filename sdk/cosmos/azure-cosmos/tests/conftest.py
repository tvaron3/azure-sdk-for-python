# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.

import test_config
from azure.cosmos import CosmosClient as CosmosSyncClient

cosmos_sync_client = CosmosSyncClient(test_config.TestConfig.host, test_config.TestConfig.masterKey)


def pytest_configure(config):
    """
    Allows plugins and conftest files to perform initial configuration.
    This hook is called for every plugin and initial conftest
    file after command line options have been parsed.
    """


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    config = test_config.TestConfig
    config.get_account_info(cosmos_sync_client)
    config.create_database_if_not_exist(cosmos_sync_client)
    config.create_single_partition_container_if_not_exist(cosmos_sync_client)
    config.create_multi_partition_container_if_not_exist(cosmos_sync_client)
    config.create_single_partition_prefix_pk_container_if_not_exist(cosmos_sync_client)
    config.create_multi_partition_prefix_pk_container_if_not_exist(cosmos_sync_client)

def pytest_sessionfinish(session, exitstatus):
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """
    config = test_config.TestConfig
    config.try_delete_database(cosmos_sync_client)


def pytest_unconfigure(config):
    """
    called before test process is exited.
    """


import pytest


@pytest.fixture(autouse=True)
def _reset_shared_pk_range_cache():
    """Reset module-level shared partition-key-range cache between tests.

    The shared cache (introduced for the cross-client memory optimisation)
    is process-global state. Without this fixture, state from one test
    (cached routing maps, per-(loop, collection) locks, refcounts) leaks
    into subsequent tests, causing order-dependent failures and flakiness
    in any test that asserts on cache contents or _ReadPartitionKeyRanges
    call counts.

    We clear after the test runs so the test under observation can still
    exercise the normal population behaviour.
    """
    yield
    # Local import to avoid pulling these modules in at conftest collection
    # time (some environments treat conftest import errors as fatal).
    from azure.cosmos._routing import routing_map_provider as _sync_pmp
    from azure.cosmos._routing.aio import routing_map_provider as _async_pmp

    with _sync_pmp._shared_cache_lock:  # pylint: disable=protected-access
        _sync_pmp._shared_routing_map_cache.clear()  # pylint: disable=protected-access
        _sync_pmp._shared_collection_locks.clear()  # pylint: disable=protected-access
        _sync_pmp._shared_locks_locks.clear()  # pylint: disable=protected-access
        _sync_pmp._shared_cache_refcounts.clear()  # pylint: disable=protected-access

    with _async_pmp._shared_cache_lock:  # pylint: disable=protected-access
        _async_pmp._shared_routing_map_cache.clear()  # pylint: disable=protected-access
        _async_pmp._shared_collection_locks.clear()  # pylint: disable=protected-access
        _async_pmp._shared_locks_locks.clear()  # pylint: disable=protected-access
        _async_pmp._shared_cache_refcounts.clear()  # pylint: disable=protected-access
