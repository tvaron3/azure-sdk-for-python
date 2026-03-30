# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.

"""

This is the ASYNC version of the fault injection test to match the customer's
exact log statements which showed:
- Source: _GetDatabaseAccount (for Read)
- Source: _database_account_check (for Write)


The SDK uses HealthCheckRetryPolicy which retries metadata requests 3 times
(default) before propagating the error. The fault injection in these tests
returns errors on EVERY request, so all 3 retries fail, triggering the
endpoint unavailability marking.

Only after 3 retries fail does _mark_endpoint_unavailable() get called
"""

import os
import sys
import unittest
import logging

from azure.core.pipeline.transport import HttpRequest
from azure.cosmos.aio import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError
from azure.cosmos.http_constants import StatusCodes

# Add tests directory to path
sys.path.insert(0, os.path.dirname(__file__))
from _fault_injection_transport_async import FaultInjectionTransportAsync
from _fault_injection_transport import FaultInjectionTransport

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Live account configuration
LIVE_ACCOUNT_HOST = os.getenv('COSMOS_MULTI_WRITE_HOST', "https://sdkdev-dikshi-account.documents.azure.com:443/")
LIVE_ACCOUNT_KEY = os.getenv('COSMOS_MULTI_WRITE_KEY', "==")


class TestIncidentFeb4ReproductionAsync(unittest.IsolatedAsyncioTestCase):
    """
    This uses the ASYNC SDK to match customer's log output:
    - Source: _GetDatabaseAccount (for Read marking)
    - Source: _database_account_check (for Write marking)
    """

    host = LIVE_ACCOUNT_HOST
    key = LIVE_ACCOUNT_KEY

    async def test_01_async_verify_multi_write_account_configuration(self):
        """Verify the live account is configured as multi-write with both regions (async)."""
        logger.info("\n" + "="*80)
        logger.info("ASYNC TEST 01: VERIFY MULTI-WRITE ACCOUNT CONFIGURATION")
        logger.info("="*80)

        async with CosmosClient(
                self.host,
                self.key,
                preferred_locations=["West US 2", "East US"],
                multiple_write_locations=True
        ) as client:
            gem = client.client_connection._global_endpoint_manager
            location_cache = gem.location_cache

            logger.info(f"\nAccount Configuration:")
            logger.info(f"  Default endpoint: {gem.DefaultEndpoint}")
            logger.info(f"  Write locations: {location_cache.account_write_locations}")
            logger.info(f"  Read locations: {location_cache.account_read_locations}")
            logger.info(f"  Multi-write enabled: {location_cache.enable_multiple_writable_locations}")
            logger.info(f"  Can use multiple write locations: {location_cache.can_use_multiple_write_locations()}")

            self.assertTrue(
                location_cache.can_use_multiple_write_locations(),
                "Account must be configured for multi-write"
            )
            logger.info("\n Account is configured for multi-write (ASYNC)")

    async def test_02_async_regional_metadata_failure_marks_endpoint_unavailable(self):
        """
        ASYNC Test: Regional metadata failure marks endpoint unavailable.

        Expected log output should show:
        - Source: _GetDatabaseAccount (for Read) - during fallback chain
        - Source: _database_account_check (for Write) - during health check
        """
        logger.info("\n" + "="*80)
        logger.info("ASYNC TEST 02: REGIONAL METADATA FAILURE -> MARKS UNAVAILABLE")
        logger.info("Expected sources: _GetDatabaseAccount (Read), _database_account_check (Write)")
        logger.info("="*80)

        transport = FaultInjectionTransportAsync()
        call_log = []

        async def inject_metadata_failures(request: HttpRequest):
            url = request.url.lower()
            is_metadata = FaultInjectionTransport.predicate_is_database_account_call(request)

            if not is_metadata:
                return None

            call_log.append(url)

            # Global endpoint - always fail to force regional fallback
            if "-eastus" not in url and "-westus" not in url:
                logger.info(f"  Global metadata FAIL: {url[:50]}...")
                return CosmosHttpResponseError(
                    status_code=StatusCodes.INTERNAL_SERVER_ERROR,
                    message="Injected Global failure",
                    response=None
                )

            # East US regional - fail to test marking unavailable
            if "-eastus" in url:
                logger.info(f"  East US metadata FAIL: {url[:50]}...")
                return CosmosHttpResponseError(
                    status_code=StatusCodes.INTERNAL_SERVER_ERROR,
                    message="Injected East US failure",
                    response=None
                )

            # West US 2 - succeed (healthy fallback)
            if "-westus" in url:
                logger.info(f"  West US 2 metadata SUCCESS: {url[:50]}...")
                return None

            return None

        transport.add_fault(
            predicate=lambda r: True,
            fault_factory=inject_metadata_failures
        )

        async with CosmosClient(
                self.host,
                self.key,
                preferred_locations=["East US", "West US 2"],
                multiple_write_locations=True,
                transport=transport
        ) as client:
            gem = client.client_connection._global_endpoint_manager
            location_cache = gem.location_cache

            logger.info(f"\nMetadata call sequence:")
            for i, url in enumerate(call_log):
                logger.info(f"  {i+1}. {url[:60]}...")

            logger.info(f"\nEndpoints marked unavailable:")
            for endpoint in location_cache.location_unavailability_info_by_endpoint.keys():
                logger.info(f"  - {endpoint}")

            eastus_unavailable = any(
                "eastus" in ep.lower()
                for ep in location_cache.location_unavailability_info_by_endpoint.keys()
            )

            if eastus_unavailable:
                logger.info("\n East US WAS MARKED UNAVAILABLE after metadata failure!")
                logger.info("  Check logs above for source names:")
                logger.info("  - Expected: Source: _GetDatabaseAccount (Read)")
                logger.info("  - Expected: Source: _database_account_check (Write)")

    async def test_03_async_400_101_unauthorized_marks_endpoint_unavailable(self):
        """
        ASYNC Test: 400:101 (Unauthorized) errors mark endpoints unavailable.

        This proves the CUSTOMER LOGS WINDOW (02:09-02:10 UTC) scenario using async SDK.

        Expected log output should show:
        - Source: _GetDatabaseAccount (for Read)
        - Source: _database_account_check (for Write)
        """
        logger.info("\n" + "="*80)
        logger.info("ASYNC TEST 03: 400:101 UNAUTHORIZED -> MARKS UNAVAILABLE")
        logger.info("This proves the CUSTOMER LOGS scenario (02:09-02:10 UTC)")
        logger.info("Expected sources: _GetDatabaseAccount (Read), _database_account_check (Write)")
        logger.info("="*80)

        transport = FaultInjectionTransportAsync()
        call_log = []

        async def inject_400_101_on_eastus(request: HttpRequest):
            url = request.url.lower()
            is_metadata = FaultInjectionTransport.predicate_is_database_account_call(request)

            if not is_metadata:
                return None

            call_log.append({"url": url, "endpoint": "Global" if "-eastus" not in url and "-westus" not in url else ("East US" if "-eastus" in url else "West US 2")})

            # Global - fail with 500 (to force regional fallback)
            if "-eastus" not in url and "-westus" not in url:
                logger.info(f"  Global: FAIL (500)")
                return CosmosHttpResponseError(
                    status_code=StatusCodes.INTERNAL_SERVER_ERROR,
                    message="Global 500",
                    response=None
                )

            # East US - fail with 400:101 (Unauthorized) - simulating the incident
            if "-eastus" in url:
                logger.info(f"  East US: FAIL (400:101 Unauthorized) -> SHOULD BE MARKED UNAVAILABLE")
                return CosmosHttpResponseError(
                    status_code=400,
                    message="Unauthorized - Injected 400:101 error simulating token expiry",
                    response=None,
                    sub_status_code=101
                )

            # West US 2 - succeed
            if "-westus" in url:
                logger.info(f"  West US 2: SUCCESS")
                return None

            return None

        transport.add_fault(
            predicate=lambda r: True,
            fault_factory=inject_400_101_on_eastus
        )

        logger.info("\nCreating async client (triggers _GetDatabaseAccount)...")
        async with CosmosClient(
                self.host,
                self.key,
                preferred_locations=["East US", "West US 2"],
                multiple_write_locations=True,
                transport=transport
        ) as client:
            gem = client.client_connection._global_endpoint_manager
            location_cache = gem.location_cache

            logger.info(f"\nCall sequence:")
            for i, call in enumerate(call_log):
                logger.info(f"  {i+1}. {call['endpoint']}: {call['url'][:50]}...")

            eastus_unavailable = any("eastus" in ep.lower() for ep in location_cache.location_unavailability_info_by_endpoint.keys())
            westus_unavailable = any("westus" in ep.lower() for ep in location_cache.location_unavailability_info_by_endpoint.keys())

            logger.info(f"\nEndpoint states after 400:101 error:")
            logger.info(f"  East US unavailable: {eastus_unavailable}")
            logger.info(f"  West US 2 unavailable: {westus_unavailable}")

            logger.info(f"\nUnavailable endpoints:")
            for ep in location_cache.location_unavailability_info_by_endpoint.keys():
                logger.info(f"  - {ep}")

            if eastus_unavailable:
                logger.info("\n 400:101 (Unauthorized) DOES MARK ENDPOINT UNAVAILABLE! (ASYNC)")
                logger.info("  This proves the customer logs scenario:")
                logger.info("  - 52 400:101 errors on WUS2 in 02:05 bucket")
                logger.info("  - SDK marked WUS2 unavailable (seen in customer logs at 02:10:19)")
                logger.info("  - ANY CosmosHttpResponseError triggers marking, not just 500s")
                logger.info("\n  Check logs above for ASYNC-specific source names:")
                logger.info("  - Source: _GetDatabaseAccount (Read)")
                logger.info("  - Source: _database_account_check (Write)")

            self.assertTrue(
                eastus_unavailable,
                "400:101 (Unauthorized) should mark endpoint unavailable"
            )

    async def test_04_async_verify_retry_behavior_success_within_retries(self):
        """
        ASYNC Test: Verify retry behavior - if request succeeds within retry limit,
        endpoint should NOT be marked unavailable.

        This test fails the first 2 requests, then succeeds on the 3rd.
        """
        logger.info("\n" + "="*80)
        logger.info("ASYNC TEST 04: RETRY BEHAVIOR - SUCCESS WITHIN RETRY LIMIT")
        logger.info("="*80)
        logger.info("HealthCheckRetryPolicy default: 3 retries")
        logger.info("Test: Fail 2 times, succeed on 3rd -> should NOT mark unavailable")

        transport = FaultInjectionTransportAsync()

        # Track retry counts per endpoint
        retry_counts = {
            "global": 0,
            "eastus": 0,
            "westus": 0
        }

        async def inject_failures_with_retry_success(request: HttpRequest):
            url = request.url.lower()
            is_metadata = FaultInjectionTransport.predicate_is_database_account_call(request)

            if not is_metadata:
                return None

            # Determine endpoint type
            if "-eastus" in url:
                endpoint_key = "eastus"
            elif "-westus" in url:
                endpoint_key = "westus"
            else:
                endpoint_key = "global"

            retry_counts[endpoint_key] += 1
            current_retry = retry_counts[endpoint_key]

            # Global - always fail to force regional fallback
            if endpoint_key == "global":
                logger.info(f"  Global attempt #{current_retry}: FAIL (500)")
                return CosmosHttpResponseError(
                    status_code=StatusCodes.INTERNAL_SERVER_ERROR,
                    message=f"Global failure attempt #{current_retry}",
                    response=None
                )

            # East US - fail first 2 attempts, succeed on 3rd
            if endpoint_key == "eastus":
                if current_retry <= 2:
                    logger.info(f"  East US attempt #{current_retry}: FAIL (500) - within retry limit")
                    return CosmosHttpResponseError(
                        status_code=StatusCodes.INTERNAL_SERVER_ERROR,
                        message=f"East US failure attempt #{current_retry}",
                        response=None
                    )
                else:
                    logger.info(f"  East US attempt #{current_retry}: SUCCESS - within retry limit!")
                    return None  # Success on 3rd attempt

            # West US 2 - succeed
            if endpoint_key == "westus":
                logger.info(f"  West US 2 attempt #{current_retry}: SUCCESS")
                return None

            return None

        transport.add_fault(
            predicate=lambda r: True,
            fault_factory=inject_failures_with_retry_success
        )

        logger.info("\nCreating async client (triggers _GetDatabaseAccount with retries)...")
        async with CosmosClient(
                self.host,
                self.key,
                preferred_locations=["East US", "West US 2"],
                multiple_write_locations=True,
                transport=transport
        ) as client:
            gem = client.client_connection._global_endpoint_manager
            location_cache = gem.location_cache

            logger.info(f"\nRetry counts:")
            for endpoint, count in retry_counts.items():
                logger.info(f"  {endpoint}: {count} attempts")

            eastus_unavailable = any("eastus" in ep.lower() for ep in location_cache.location_unavailability_info_by_endpoint.keys())

            logger.info(f"\nEndpoint states after retries:")
            logger.info(f"  East US unavailable: {eastus_unavailable}")

            if not eastus_unavailable:
                logger.info("\n CORRECT: East US NOT marked unavailable! (ASYNC)")
                logger.info("  Retry behavior verified: succeeded within retry limit")
            else:
                logger.info("\n UNEXPECTED: East US was marked unavailable")

            # Assert: East US should NOT be marked unavailable
            self.assertFalse(
                eastus_unavailable,
                "East US should NOT be marked unavailable - succeeded within retry limit"
            )

    async def test_05_async_verify_retry_behavior_failure_exceeds_retries(self):
        """
        ASYNC Test: Verify endpoint IS marked unavailable when all retries are exhausted.
        """
        logger.info("\n" + "="*80)
        logger.info("ASYNC TEST 05: RETRY BEHAVIOR - FAILURE EXCEEDS RETRY LIMIT")
        logger.info("="*80)
        logger.info("HealthCheckRetryPolicy default: 3 retries")
        logger.info("Test: Fail all attempts -> SHOULD mark unavailable")

        transport = FaultInjectionTransportAsync()

        # Track retry counts per endpoint
        retry_counts = {
            "global": 0,
            "eastus": 0,
            "westus": 0
        }

        async def inject_all_failures_then_westus_success(request: HttpRequest):
            url = request.url.lower()
            is_metadata = FaultInjectionTransport.predicate_is_database_account_call(request)

            if not is_metadata:
                return None

            # Determine endpoint type
            if "-eastus" in url:
                endpoint_key = "eastus"
            elif "-westus" in url:
                endpoint_key = "westus"
            else:
                endpoint_key = "global"

            retry_counts[endpoint_key] += 1
            current_retry = retry_counts[endpoint_key]

            # Global - always fail
            if endpoint_key == "global":
                logger.info(f"  Global attempt #{current_retry}: FAIL (500)")
                return CosmosHttpResponseError(
                    status_code=StatusCodes.INTERNAL_SERVER_ERROR,
                    message=f"Global failure attempt #{current_retry}",
                    response=None
                )

            # East US - ALWAYS fail (exhaust all retries)
            if endpoint_key == "eastus":
                logger.info(f"  East US attempt #{current_retry}: FAIL (500) - exhausting retries")
                return CosmosHttpResponseError(
                    status_code=StatusCodes.INTERNAL_SERVER_ERROR,
                    message=f"East US failure attempt #{current_retry}",
                    response=None
                )

            # West US 2 - succeed (fallback)
            if endpoint_key == "westus":
                logger.info(f"  West US 2 attempt #{current_retry}: SUCCESS (fallback)")
                return None

            return None

        transport.add_fault(
            predicate=lambda r: True,
            fault_factory=inject_all_failures_then_westus_success
        )

        logger.info("\nCreating async client (triggers _GetDatabaseAccount with retries)...")
        async with CosmosClient(
                self.host,
                self.key,
                preferred_locations=["East US", "West US 2"],
                multiple_write_locations=True,
                transport=transport
        ) as client:
            gem = client.client_connection._global_endpoint_manager
            location_cache = gem.location_cache

            logger.info(f"\nRetry counts:")
            for endpoint, count in retry_counts.items():
                logger.info(f"  {endpoint}: {count} attempts")

            eastus_unavailable = any("eastus" in ep.lower() for ep in location_cache.location_unavailability_info_by_endpoint.keys())

            logger.info(f"\nEndpoint states after exhausted retries:")
            logger.info(f"  East US unavailable: {eastus_unavailable}")

            if eastus_unavailable:
                logger.info("\n CORRECT: East US IS marked unavailable! (ASYNC)")
                logger.info(f"  Retry behavior verified: all {retry_counts['eastus']} attempts failed")
            else:
                logger.info("\n UNEXPECTED: East US was NOT marked unavailable")

            # Assert: East US SHOULD be marked unavailable
            self.assertTrue(
                eastus_unavailable,
                "East US SHOULD be marked unavailable - all retries exhausted"
            )
    async def test_writes_fallback_to_global_when_regional_unavailable_and_other_excluded(self):
        """
        When:
        - Region A (East US) is marked unavailable for WRITE
        - Region B (West US 2) is excluded via excluded_locations

        Then:
        - SDK resolves writes to GLOBAL endpoint (only remaining option)
        - This explains why fe87 received 11M+ writes during the incident

        Why we manually mark unavailable instead of using fault injection:
        There is a TIMING ISSUE in _mark_endpoint_unavailable() during client startup:

            def _mark_endpoint_unavailable(self, endpoint, context):
                write_endpoints = self.location_cache.get_all_write_endpoints()  # Only {global} at startup!
                self.mark_endpoint_unavailable_for_read(endpoint, False, context)  #  Always called
                if endpoint in write_endpoints:  # East US NOT in write_endpoints yet!
                    self.mark_endpoint_unavailable_for_write(endpoint, False, context)  #  SKIPPED!

        During client startup, write_endpoints is initialized to just {global}. When metadata
        fails on East US during _GetDatabaseAccount fallback, East US is not in write_endpoints
        yet, so it only gets marked unavailable for READ (not WRITE).

        In the ACTUAL INCIDENT:
        - SDK instances were ALREADY RUNNING (location cache was populated with both regions)
        - When metadata failed, East US WAS in write_endpoints
        - So East US got marked unavailable for BOTH Read AND Write

        This test accurately reproduces the incident by:
        1. Letting the client initialize normally (cache gets populated with both regions)
        2. THEN manually marking East US unavailable for Write (simulating post-startup failure)
        3. Verifying SDK falls back to GLOBAL when West US 2 is also excluded
        """
        # Step 1: Initialize client normally - this populates the location cache with both regions
        # At this point, write_endpoints = {eastus, westus2} (not just {global})
        async with CosmosClient(
                self.host, self.key,
                preferred_locations=["East US", "West US 2"],
                multiple_write_locations=True
        ) as client:
            location_cache = client.client_connection._global_endpoint_manager.location_cache
            write_endpoints = location_cache.get_all_write_endpoints()

            # Step 2: Find East US endpoint and mark it unavailable for BOTH Read AND Write
            # This simulates what happens when metadata fails AFTER the cache is already populated
            # (which is the scenario in the actual incident - SDK instances were already running)
            eastus_endpoint = next((ep for ep in write_endpoints if "eastus" in ep.lower()), None)
            self.assertIsNotNone(eastus_endpoint, "East US should be in write endpoints")

            location_cache.mark_endpoint_unavailable_for_read(eastus_endpoint, False, "test")
            location_cache.mark_endpoint_unavailable_for_write(eastus_endpoint, False, "test")
            location_cache.update_location_cache()  # Re-filter write_regional_routing_contexts

            # Step 3: Create a mock write request with excluded_locations=['West US 2']
            # This simulates the customer's configuration that excluded WUS2 for writes
            from azure.cosmos._request_object import RequestObject
            from azure.cosmos.http_constants import ResourceType
            from azure.cosmos.documents import _OperationType

            mock_request = RequestObject(ResourceType.Document, _OperationType.Create, {})
            mock_request.excluded_locations = ["West US 2"]

            # Step 4: Get applicable write contexts - this is the SDK's endpoint resolution logic
            # With East US unavailable and West US 2 excluded, only GLOBAL should remain
            applicable_contexts = location_cache._get_applicable_write_regional_routing_contexts(mock_request)

            # Step 5: Verify the only available endpoint is GLOBAL
            self.assertEqual(len(applicable_contexts), 1, "Should have exactly one fallback endpoint")

            endpoint = applicable_contexts[0].get_primary()
            is_global = "-eastus" not in endpoint.lower() and "-westus" not in endpoint.lower()

            self.assertTrue(is_global, f"Expected GLOBAL endpoint, got: {endpoint}")



if __name__ == "__main__":
    unittest.main(verbosity=2)

