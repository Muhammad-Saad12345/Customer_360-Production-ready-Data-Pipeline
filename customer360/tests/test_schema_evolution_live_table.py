"""Schema evolution must compare against the live Glue table, not only the code.

Regression: staging oms_orders was created by an older release without
customer_email; the loader trusted the static definition, skipped the ALTER,
and the MERGE failed with COLUMN_NOT_FOUND.
"""
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from src.lakehouse.iceberg_loader import IcebergLoader
from src.lakehouse.table_definitions import get_table_definition


def _loader(live_columns):
    glue = MagicMock()
    glue.get_table_metadata.return_value = {
        "StorageDescriptor": {"Columns": [{"Name": c} for c in live_columns]},
        "PartitionKeys": [],
    }
    athena = MagicMock()
    config = MagicMock()
    config.database_name = "customer360_iceberg_stage"
    return IcebergLoader(config=config, glue_catalog=glue, athena_client=athena, s3_client=MagicMock())


def test_defined_column_missing_from_live_table_is_added():
    table_def = get_table_definition("oms", "orders")
    live = [c for c in table_def.column_names if c != "customer_email"]
    loader = _loader(live)
    schema = pa.schema([(c, pa.string()) for c in table_def.column_names])

    evolved = loader.handle_schema_evolution(table_def, schema)

    assert [c.name for c in evolved] == ["customer_email"]
    assert evolved[0].data_type == "string"
    ddl = loader.athena.run_ddl.call_args[0][0]
    assert "ALTER TABLE customer360_iceberg_stage.oms_orders ADD COLUMNS (customer_email string)" in ddl


def test_up_to_date_table_needs_no_ddl():
    table_def = get_table_definition("oms", "orders")
    loader = _loader(table_def.column_names)
    schema = pa.schema([(c, pa.string()) for c in table_def.column_names])

    assert loader.handle_schema_evolution(table_def, schema) == []
    loader.athena.run_ddl.assert_not_called()


def test_failed_alter_stops_the_load_with_a_clear_error():
    table_def = get_table_definition("oms", "orders")
    loader = _loader([c for c in table_def.column_names if c != "customer_email"])
    loader.athena.run_ddl.side_effect = RuntimeError("AccessDenied glue:UpdateTable")

    with pytest.raises(RuntimeError, match="customer_email"):
        loader.handle_schema_evolution(table_def, pa.schema([("order_id", pa.string())]))
