# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.

"""Async unit test for the ``feed_range`` query page-size honoring fix.

Mirror of ``test_query_feed_range_max_item_count.py`` for the async
``CosmosClientConnection`` in ``azure.cosmos.aio``.

Note: these tests reach into the name-mangled
``_CosmosClientConnection__QueryFeed`` / ``_CosmosClientConnection__Post``
members.  If ``__QueryFeed`` is renamed or moved off the async
``CosmosClientConnection``, move these tests with it.
"""

import unittest
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from azure.cosmos.aio._cosmos_client_connection_async import CosmosClientConnection
from azure.cosmos._change_feed.feed_range_internal import FeedRangeInternalEpk
from azure.cosmos._routing.routing_range import Range


def _build_async_client_connection(overlapping_ranges=None):
    """Build a bare async ``CosmosClientConnection`` instance with only the
    attributes referenced by ``__QueryFeed``'s feed_range branch."""
    client = object.__new__(CosmosClientConnection)
    client.default_headers = {}
    client._query_compatibility_mode = CosmosClientConnection._QueryCompatibilityMode.Default
    client.availability_strategy = None
    client.availability_strategy_executor = None
    client.availability_strategy_max_concurrency = None
    client.last_response_headers = {}
    if overlapping_ranges is None:
        overlapping_ranges = [
            {"id": "0", "minInclusive": "", "maxExclusive": "55"},
            {"id": "1", "minInclusive": "55", "maxExclusive": "AA"},
            {"id": "2", "minInclusive": "AA", "maxExclusive": "FF"},
        ]
    client._routing_map_provider = MagicMock()
    client._routing_map_provider.get_overlapping_ranges = AsyncMock(return_value=overlapping_ranges)
    client._UpdateSessionIfRequired = MagicMock()
    return client


def _make_feed_range_dict():
    return FeedRangeInternalEpk(
        Range(range_min="", range_max="FF", isMinInclusive=True, isMaxInclusive=False)
    ).to_dict()


def _docs(n, prefix="d"):
    return {"Documents": [{"id": f"{prefix}-{i}"} for i in range(n)]}


def _capture_result_fn():
    captured = {}

    def fn(result):
        captured["result"] = result
        return result["Documents"]
    return captured, fn


@pytest.mark.asyncio
@patch("azure.cosmos.aio._cosmos_client_connection_async.base.set_session_token_header_async",
       new=AsyncMock(return_value=None))
@patch("azure.cosmos.aio._cosmos_client_connection_async.base.GetHeaders",
       side_effect=lambda *args, **kwargs: {})
class TestQueryFeedRangeMaxItemCountAsync:

    async def _query(self, client, options, post_side_effect):
        post_mock = AsyncMock(side_effect=post_side_effect)
        client._CosmosClientConnection__Post = post_mock
        captured, result_fn = _capture_result_fn()
        docs = await client._CosmosClientConnection__QueryFeed(
            path="/dbs/db1/colls/coll1/docs",
            resource_type="docs",
            id_="coll1",
            result_fn=result_fn,
            create_fn=None,
            query={"query": "SELECT * FROM c"},
            options=options,
            feed_range=_make_feed_range_dict(),
        )
        return docs, post_mock, captured

    async def test_first_page_truncated_to_max_item_count(self, _mock_get_headers):
        client = _build_async_client_connection()
        page_size = 5
        docs, post_mock, captured = await self._query(
            client,
            options={"maxItemCount": page_size},
            post_side_effect=lambda *a, **kw: (_docs(page_size), {}),
        )
        assert post_mock.call_count == 3
        assert len(docs) == page_size
        assert len(captured["result"]["Documents"]) == page_size

    async def test_truncation_to_one_across_three_ranges(self, _mock_get_headers):
        client = _build_async_client_connection()
        docs, _post_mock, captured = await self._query(
            client,
            options={"maxItemCount": 1},
            post_side_effect=lambda *a, **kw: (_docs(5), {}),
        )
        assert len(docs) == 1
        assert len(captured["result"]["Documents"]) == 1

    async def test_no_truncation_when_under_cap(self, _mock_get_headers):
        client = _build_async_client_connection()
        docs, _post_mock, _captured = await self._query(
            client,
            options={"maxItemCount": 10},
            post_side_effect=lambda *a, **kw: (_docs(1), {}),
        )
        assert len(docs) == 3

    async def test_boundary_exact_cap_no_slice(self, _mock_get_headers):
        client = _build_async_client_connection()
        docs, _post_mock, _captured = await self._query(
            client,
            options={"maxItemCount": 3},
            post_side_effect=lambda *a, **kw: (_docs(1), {}),
        )
        assert len(docs) == 3

    async def test_no_max_item_count_no_truncation(self, _mock_get_headers):
        client = _build_async_client_connection()
        docs, _post_mock, _captured = await self._query(
            client,
            options={},
            post_side_effect=lambda *a, **kw: (_docs(4), {}),
        )
        assert len(docs) == 12

    async def test_max_item_count_zero_means_server_default_no_truncation(self, _mock_get_headers):
        """maxItemCount=0 means "use the server default page size", not
        "return zero items".  See the corresponding sync test for the
        full rationale."""
        client = _build_async_client_connection()
        docs, _post_mock, _captured = await self._query(
            client,
            options={"maxItemCount": 0},
            post_side_effect=lambda *a, **kw: (_docs(7), {}),
        )
        assert len(docs) == 21

    async def test_single_overlapping_range_unchanged(self, _mock_get_headers):
        client = _build_async_client_connection(overlapping_ranges=[
            {"id": "0", "minInclusive": "", "maxExclusive": "FF"},
        ])
        docs, post_mock, _captured = await self._query(
            client,
            options={"maxItemCount": 5},
            post_side_effect=lambda *a, **kw: (_docs(5), {}),
        )
        assert post_mock.call_count == 1
        assert len(docs) == 5

    async def test_missing_documents_key_does_not_crash(self, _mock_get_headers):
        """A partial result missing the Documents key entirely must not raise
        from the truncation block."""
        client = _build_async_client_connection(overlapping_ranges=[
            {"id": "0", "minInclusive": "", "maxExclusive": "FF"},
        ])
        post_mock = AsyncMock(side_effect=lambda *a, **kw: ({"some_other_field": 42}, {}))
        client._CosmosClientConnection__Post = post_mock
        captured = {}

        def lenient_result_fn(result):
            captured["result"] = result
            return result.get("Documents") or []

        docs = await client._CosmosClientConnection__QueryFeed(
            path="/dbs/db1/colls/coll1/docs",
            resource_type="docs",
            id_="coll1",
            result_fn=lenient_result_fn,
            create_fn=None,
            query={"query": "SELECT * FROM c"},
            options={"maxItemCount": 5},
            feed_range=_make_feed_range_dict(),
        )
        assert docs == []
        assert "Documents" not in captured["result"]

    async def test_truncation_suppresses_continuation_header(self, _mock_get_headers):
        """When the merged page is truncated, the surfaced continuation token
        only describes the last inner PK range and would silently skip
        documents on resume. It must be stripped from last_response_headers."""
        client = _build_async_client_connection()
        docs, _post_mock, _captured = await self._query(
            client,
            options={"maxItemCount": 5},
            post_side_effect=lambda *a, **kw: (
                _docs(5), {"x-ms-continuation": "inner-token"}
            ),
        )
        assert len(docs) == 5
        assert "x-ms-continuation" not in client.last_response_headers, \
            "continuation header must be stripped on truncation"

    async def test_no_truncation_preserves_continuation_header(self, _mock_get_headers):
        """When the merged page fits within the cap, no truncation happens,
        so the inner continuation must be left intact."""
        client = _build_async_client_connection()
        docs, _post_mock, _captured = await self._query(
            client,
            # 3 ranges * 2 docs = 6, cap=10 -> no truncation
            options={"maxItemCount": 10},
            post_side_effect=lambda *a, **kw: (
                _docs(2), {"x-ms-continuation": "inner-token"}
            ),
        )
        assert len(docs) == 6
        assert client.last_response_headers.get("x-ms-continuation") == "inner-token"

