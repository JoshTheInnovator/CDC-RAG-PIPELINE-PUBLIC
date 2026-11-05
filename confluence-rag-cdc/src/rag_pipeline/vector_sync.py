"""Create (first run) or sync (later runs) the Delta Sync index on the silver table."""
from databricks.vector_search.client import VectorSearchClient

from .common import parse_args


def main(argv=None):
    cfg = parse_args(argv)
    vsc = VectorSearchClient(disable_notice=True)

    endpoints = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
    if cfg.vs_endpoint not in endpoints:
        vsc.create_endpoint_and_wait(name=cfg.vs_endpoint, endpoint_type="STANDARD")

    existing = [i["name"] for i in vsc.list_indexes(name=cfg.vs_endpoint).get("vector_indexes", [])]
    if cfg.index_name in existing:
        vsc.get_index(endpoint_name=cfg.vs_endpoint, index_name=cfg.index_name).sync()
        print(f"sync triggered for {cfg.index_name}")
    else:
        vsc.create_delta_sync_index_and_wait(
            endpoint_name=cfg.vs_endpoint,
            index_name=cfg.index_name,
            source_table_name=cfg.silver_table,
            primary_key="chunk_id",
            pipeline_type="TRIGGERED",
            embedding_source_column="content",
            embedding_model_endpoint_name=cfg.embedding_endpoint,
        )
        print(f"created {cfg.index_name}")


if __name__ == "__main__":
    main()
