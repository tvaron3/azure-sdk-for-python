# The MIT License (MIT)
# Copyright (c) Microsoft Corporation. All rights reserved.

"""Sync unit test for the ``feed_range`` query page-size honoring fix.

When a user-supplied ``feed_range`` overlaps multiple physical PK ranges (for
example, after a server-side split), ``__QueryFeed`` issues one POST per
overlapping range and merges the partial results.  The user-requested
``max_item_count`` was previously honored *per inner range*, so a single page
could return up to ``K * max_item_count`` documents (where ``K`` is the number
of overlapping physical ranges).

This test pins the post-merge truncation that caps the page at the
user-requested ``max_item_count``.

Note: these tests reach into the name-mangled
``_CosmosClientConnection__QueryFeed`` / ``_CosmosClientConnection__Post``
members.  If ``__QueryFeed`` is renamed or moved off
``CosmosClientConnection``, move these tests with it.
"""

import unittest
from unittest.mock import MagicMock, patch

from azure.cosmos._cosmos_client_connection import CosmosClientConnection
from azure.cosmos._change_feed.feed_range_internal import FeedRangeInternalEpk
from azure.cosmos._routing.routing_range import Range


def _build_client_connection(overlapping_ranges=None):
    """Build a bare ``CosmosClientConnection`` instance with only the attributes
    referenced by ``__QueryFeed``'s feed_range branch.

    We deliberately bypass ``__init__`` so the test does not require an
    emulator or any network setup.
    """
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
    client._routing_map_provider.get_overlapping_ranges.return_value = overlapping_ranges
    client._UpdateSessionIfRequired = MagicMock()
    return client


def _make_feed_range_dict():
    """Return a feed_range JSON-serializable dict that spans the full hash space."""
    return FeedRangeInternalEpk(
        Range(range_min="", range_max="FF", isMinInclusive=True, isMaxInclusive=False)
    ).to_dict()


def _docs(n, prefix="d"):
    return {"Documents": [{"id": f"{prefix}-{i}"} for i in range(n)]}


def _capture_result_fn():
    """A ``result_fn`` that records the dict it is called with so tests can assert
    that the *underlying merged dict* (not just the projection) was truncated."""
    captured = {}

    def fn(result):
        captured["result"] = result
        return result["Documents"]
    return captured, fn


@patch("azure.cosmos._cosmos_client_connection.base.set_session_token_header",
       lambda *args, **kwargs: None)
@patch("azure.cosmos._cosmos_client_connection.base.GetHeaders",
       side_effect=lambda *args, **kwargs: {})
class TestQueryFeedRangeMaxItemCount(unittest.TestCase):

    def _query(self, client, options, post_side_effect):
        post_mock = MagicMock(side_effect=post_side_effect)
        client._CosmosClientConnection__Post = post_mock
        captured, result_fn = _capture_result_fn()
        docs, _headers = client._CosmosClientConnection__QueryFeed(
            path="/dbs/db1/colls/coll1/docs",
            resource_type="docs",
            resource_id="coll1",
            result_fn=result_fn,
            create_fn=None,
            query={"query": "SELECT * FROM c"},
            options=options,
            feed_range=_make_feed_range_dict(),
        )
        return docs, post_mock, captured

    def test_first_page_truncated_to_max_item_count(self, _mock_get_headers):
        """A single page must not exceed ``max_item_count`` even when multiple
        physical PK ranges overlap the requested feed_range."""
        client = _build_client_connection()
        page_size = 5
        docs, post_mock, captured = self._query(
            client,
            options={"maxItemCount": page_size},
            post_side_effect=lambda *a, **kw: (_docs(page_size), {}),
        )
        # All three inner ranges queried (intentional — see the follow-up note
        # about composite continuation tokens).
        self.assertEqual(post_mock.call_count, 3)
        # Both the projection and the merged dict are capped.
        self.assertEqual(len(docs), page_size)
        self.assertEqual(len(captured["result"]["Documents"]), page_size)

    def test_truncation_to_one_across_three_ranges(self, _mock_get_headers):
        """Tightest cap: K=3, N=1 — proves we truncate, not "merge correctly"."""
        client = _build_client_connection()
        docs, _post_mock, captured = self._query(
            client,
            options={"maxItemCount": 1},
            post_side_effect=lambda *a, **kw: (_docs(5), {}),
        )
        self.assertEqual(len(docs), 1)
        self.assertEqual(len(captured["result"]["Documents"]), 1)

    def test_no_truncation_when_under_cap(self, _mock_get_headers):
        """If the merged result is already <= max_item_count, nothing is dropped."""
        client = _build_client_connection()
        docs, _post_mock, _captured = self._query(
            client,
            options={"maxItemCount": 10},
            post_side_effect=lambda *a, **kw: (_docs(1), {}),
        )
        self.assertEqual(len(docs), 3)

    def test_boundary_exact_cap_no_slice(self, _mock_get_headers):
        """When merged length == cap, the list is returned unchanged."""
        client = _build_client_connection()
        # 3 ranges * 1 doc = 3 merged; cap = 3.
        docs, _post_mock, _captured = self._query(
            client,
            options={"maxItemCount": 3},
            post_side_effect=lambda *a, **kw: (_docs(1), {}),
        )
        self.assertEqual(len(docs), 3)

    def test_no_max_item_count_no_truncation(self, _mock_get_headers):
        """When no maxItemCount is supplied, the merged page is returned in full."""
        client = _build_client_connection()
        docs, _post_mock, _captured = self._query(
            client,
            options={},
            post_side_effect=lambda *a, **kw: (_docs(4), {}),
        )
        # 3 ranges * 4 docs each = 12, no truncation since maxItemCount is unset.
        self.assertEqual(len(docs), 12)

    def test_max_item_count_zero_means_server_default_no_truncation(self, _mock_get_headers):
        """maxItemCount=0 mirrors _base.GetHeaders' truthy contract: it means
        "use the server default page size", not "return zero items".  The
        truncation block must be a no-op so we don't silently empty a page
        whose docs were actually fetched at server cost."""
        client = _build_client_connection()
        docs, _post_mock, _captured = self._query(
            client,
            options={"maxItemCount": 0},
            post_side_effect=lambda *a, **kw: (_docs(7), {}),
        )
        # 3 ranges * 7 docs each = 21, no truncation since cap is non-positive.
        self.assertEqual(len(docs), 21)

    def test_single_overlapping_range_unchanged(self, _mock_get_headers):
        """Single-range feed_range case: the truncation must not regress the
        existing behavior (one POST, return the partial result as-is)."""
        client = _build_client_connection(overlapping_ranges=[
            {"id": "0", "minInclusive": "", "maxExclusive": "FF"},
        ])
        docs, post_mock, _captured = self._query(
            client,
            options={"maxItemCount": 5},
            post_side_effect=lambda *a, **kw: (_docs(5), {}),
        )
        self.assertEqual(post_mock.call_count, 1)
        self.assertEqual(len(docs), 5)

    def test_missing_documents_key_does_not_crash(self, _mock_get_headers):
        """A partial result missing the Documents key entirely must not raise
        from the truncation block; the ``isinstance(docs, list)`` guard
        rejects ``None`` and the block is a no-op."""
        client = _build_client_connection(overlapping_ranges=[
            {"id": "0", "minInclusive": "", "maxExclusive": "FF"},
        ])
        post_mock = MagicMock(side_effect=lambda *a, **kw: ({"some_other_field": 42}, {}))
        client._CosmosClientConnection__Post = post_mock
        captured = {}

        def lenient_result_fn(result):
            captured["result"] = result
            # Mimic real-world result_fns that defensively project; the point
            # of this test is that the truncation block itself does not raise
            # when Documents is missing.
            return result.get("Documents") or []

        # Should not raise.
        docs, _headers = client._CosmosClientConnection__QueryFeed(
            path="/dbs/db1/colls/coll1/docs",
            resource_type="docs",
            resource_id="coll1",
            result_fn=lenient_result_fn,
            create_fn=None,
            query={"query": "SELECT * FROM c"},
            options={"maxItemCount": 5},
            feed_range=_make_feed_range_dict(),
        )
        self.assertEqual(docs, [])
        self.assertNotIn("Documents", captured["result"])


    def test_truncation_keeps_count_field_consistent(self, _mock_get_headers):
        """After truncation, ``results['_count']`` (set by _merge_query_results)
        must be updated to match the truncated Documents length so any
        downstream introspection sees a coherent shape."""
        client = _build_client_connection()
        docs, _post_mock, captured = self._query(
            client,
            options={"maxItemCount": 5},
            post_side_effect=lambda *a, **kw: (_docs(5), {}),
        )
        self.assertEqual(len(docs), 5)
        self.assertEqual(captured["result"].get("_count"), 5,
                         "_count must be updated alongside Documents")


if __name__ == "__main__":
    unittest.main()

