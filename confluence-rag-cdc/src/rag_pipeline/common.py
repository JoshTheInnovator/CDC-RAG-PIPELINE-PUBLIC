import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    catalog: str
    schema: str
    raw_path: str
    checkpoint_root: str
    vs_endpoint: str
    embedding_endpoint: str
    chunk_size: int
    chunk_overlap: int

    @property
    def bronze_table(self) -> str:
        return f"{self.catalog}.{self.schema}.confluence_bronze"

    @property
    def silver_table(self) -> str:
        return f"{self.catalog}.{self.schema}.confluence_silver_chunks"

    @property
    def index_name(self) -> str:
        return f"{self.catalog}.{self.schema}.confluence_chunks_index"


def parse_args(argv=None) -> Config:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--raw_path", required=True, help="s3://bucket/raw/ (UC external location)")
    p.add_argument("--checkpoint_root", required=True, help="/Volumes/<cat>/<schema>/<vol>/checkpoints")
    p.add_argument("--vs_endpoint", required=True)
    p.add_argument("--embedding_endpoint", default="databricks-gte-large-en")
    p.add_argument("--chunk_size", type=int, default=1000)
    p.add_argument("--chunk_overlap", type=int, default=150)
    return Config(**vars(p.parse_args(argv)))
