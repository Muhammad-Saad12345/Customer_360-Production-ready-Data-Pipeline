from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from src.lakehouse.config import LakehouseConfig, get_lakehouse_config

logger = logging.getLogger("customer360.lakehouse.athena")


@dataclass
class AthenaQueryResult:
    query_execution_id: str
    state: str
    columns: List[str]
    rows: List[Dict[str, Any]]
    raw_response: Dict[str, Any]
    execution_time_ms: int = 0
    data_scanned_bytes: int = 0


class AthenaQueryExecutionError(Exception):
    def __init__(self, query_id: str, state: str, reason: str, query: str):
        super().__init__(f"Athena query {query_id} failed with state '{state}': {reason}\nQuery: {query}")
        self.query_id = query_id
        self.state = state
        self.reason = reason
        self.query = query


class AthenaClient:
    """
    Amazon Athena query execution client.
    Handles DDL and DML operations against Apache Iceberg tables backed by AWS Glue Data Catalog.
    """

    def __init__(self, config: Optional[LakehouseConfig] = None, athena_client: Optional[Any] = None):
        self.config = config or get_lakehouse_config()
        self.client = athena_client or boto3.client("athena", region_name=self.config.region_name)

    def execute_query(
        self,
        query: str,
        database: Optional[str] = None,
        poll_interval_seconds: float = 0.5,
        max_timeout_seconds: float = 120.0,
    ) -> AthenaQueryResult:
        """
        Execute an Athena SQL statement synchronously and wait for completion.
        Returns parsed AthenaQueryResult.
        """
        db = database or self.config.database_name
        logger.debug("Executing Athena query in db '%s':\n%s", db, query)

        params: Dict[str, Any] = {
            "QueryString": query,
            "QueryExecutionContext": {"Database": db},
            "ResultConfiguration": {"OutputLocation": self.config.athena_output_location},
            "WorkGroup": self.config.athena_workgroup,
        }

        try:
            start_resp = self.client.start_query_execution(**params)
        except ClientError as e:
            logger.error("Failed to start Athena query: %s", str(e))
            raise

        query_id = start_resp["QueryExecutionId"]
        logger.info("Athena query submitted [id=%s]", query_id)

        # Poll query state
        start_time = time.time()
        while True:
            elapsed = time.time() - start_time
            if elapsed > max_timeout_seconds:
                self.client.stop_query_execution(QueryExecutionId=query_id)
                raise TimeoutError(f"Athena query {query_id} exceeded maximum timeout of {max_timeout_seconds}s")

            status_resp = self.client.get_query_execution(QueryExecutionId=query_id)
            execution = status_resp.get("QueryExecution", {})
            status = execution.get("Status", {})
            state = status.get("State", "UNKNOWN")

            if state == "SUCCEEDED":
                stats = execution.get("Statistics", {})
                time_ms = stats.get("TotalExecutionTimeInMillis", 0)
                scanned = stats.get("DataScannedInBytes", 0)
                logger.info("Athena query [id=%s] SUCCEEDED in %d ms (scanned %d bytes)", query_id, time_ms, scanned)
                return self._fetch_results(query_id, time_ms, scanned, status_resp)

            if state in ("FAILED", "CANCELLED"):
                reason = status.get("StateChangeReason", "Unknown failure reason")
                logger.error("Athena query [id=%s] %s: %s", query_id, state, reason)
                raise AthenaQueryExecutionError(query_id, state, reason, query)

            time.sleep(poll_interval_seconds)

    def _fetch_results(
        self,
        query_id: str,
        execution_time_ms: int,
        data_scanned_bytes: int,
        execution_resp: Dict[str, Any],
    ) -> AthenaQueryResult:
        """Fetch and parse tabular results for a succeeded query."""
        paginator = self.client.get_paginator("get_query_results")
        columns: List[str] = []
        rows: List[Dict[str, Any]] = []
        is_first_page = True

        for page in paginator.paginate(QueryExecutionId=query_id):
            result_set = page.get("ResultSet", {})
            column_info = result_set.get("ResultSetMetadata", {}).get("ColumnInfo", [])

            if is_first_page and column_info:
                columns = [col.get("Name", f"col_{i}") for i, col in enumerate(column_info)]

            raw_rows = result_set.get("Rows", [])
            start_idx = 1 if is_first_page and raw_rows else 0
            is_first_page = False

            for raw_row in raw_rows[start_idx:]:
                data_cells = raw_row.get("Data", [])
                row_dict: Dict[str, Any] = {}
                for idx, cell in enumerate(data_cells):
                    col_name = columns[idx] if idx < len(columns) else f"col_{idx}"
                    row_dict[col_name] = cell.get("VarCharValue")
                rows.append(row_dict)

        return AthenaQueryResult(
            query_execution_id=query_id,
            state="SUCCEEDED",
            columns=columns,
            rows=rows,
            raw_response=execution_resp,
            execution_time_ms=execution_time_ms,
            data_scanned_bytes=data_scanned_bytes,
        )

    def run_ddl(self, ddl: str, database: Optional[str] = None) -> AthenaQueryResult:
        """Execute a DDL statement e.g. CREATE TABLE, ALTER TABLE."""
        return self.execute_query(ddl, database=database)

    def query_scalar(self, sql: str, database: Optional[str] = None) -> Any:
        """Execute query returning a single scalar value (e.g. COUNT(*))."""
        res = self.execute_query(sql, database=database)
        if res.rows and len(res.columns) > 0:
            first_col = res.columns[0]
            val = res.rows[0].get(first_col)
            try:
                return int(val)
            except (ValueError, TypeError):
                return val
        return None
