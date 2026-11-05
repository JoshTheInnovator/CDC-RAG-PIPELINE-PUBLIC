"""Smoke-test retrieval:  python scripts/query_index.py "how do we rotate secrets?" """
import sys

from databricks.vector_search.client import VectorSearchClient

ENDPOINT = "confluence-rag-endpoint"
INDEX = "main.confluence_rag.confluence_chunks_index"

idx = VectorSearchClient(disable_notice=True).get_index(endpoint_name=ENDPOINT, index_name=INDEX)
res = idx.similarity_search(
    query_text=" ".join(sys.argv[1:]),
    columns=["chunk_id", "title", "url", "content"],
    num_results=5,
)
for row in res["result"]["data_array"]:
    print(f"- {row[1]}  ({row[2]})\n  {row[3][:200]!r}\n")
