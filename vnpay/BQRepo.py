from typing import Callable

from returns.io import IOResultE, impure_safe

from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()


def load(
    dataset: str,
    table: str,
    schema: list[dict],
) -> Callable[[list[dict]], IOResultE[int]]:
    @impure_safe
    def _load(rows: list[dict]) -> int:
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{dataset}.{table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=schema,
                ),
            )
            .result()
            .output_rows
        )
        return output_rows

    return _load
