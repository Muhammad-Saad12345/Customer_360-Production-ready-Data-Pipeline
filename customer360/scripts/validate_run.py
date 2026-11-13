"""Check what a pipeline run actually wrote, using Athena (read-only queries).

    python scripts/validate_run.py --env staging --run-id c360-<execution-name>
    python scripts/validate_run.py --env staging --run-id c360-... --dbt   # also check the marts

For each Iceberg table it reports the rows loaded by the run and fails if a
primary key appears more than once in the table. With --dbt it also checks
that recognised revenue is the same in fact_order, customer_360 and
mart_executive_kpis. Exit code 1 if any check fails.

Needs athena:StartQueryExecution/GetQueryExecution/GetQueryResults on the
environment's workgroup, glue:GetTable*, and s3 read on the lakehouse bucket
(the c360-operator role has all of these).
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.lakehouse.table_definitions import TABLE_REGISTRY

ENVIRONMENTS = {
    "staging": {"database": "customer360_iceberg_stage", "workgroup": "customer360-staging"},
    "prod": {"database": "customer360_prod", "workgroup": "customer360-prod"},
}


class Athena:
    def __init__(self, workgroup: str, region: str):
        self.client = boto3.client("athena", region_name=region)
        self.workgroup = workgroup

    def scalar_rows(self, sql: str) -> list[list[str]]:
        qid = self.client.start_query_execution(QueryString=sql, WorkGroup=self.workgroup)["QueryExecutionId"]
        while True:
            state = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if state["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)
        if state["State"] != "SUCCEEDED":
            raise RuntimeError(f"{state['State']}: {state.get('StateChangeReason', '')}\n{sql}")
        rows = self.client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]
        return [[c.get("VarCharValue") for c in r["Data"]] for r in rows]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", choices=sorted(ENVIRONMENTS), required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dbt", action="store_true", help="also check revenue across the dbt marts")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()
    if not args.run_id.replace("-", "").replace("_", "").isalnum():
        parser.error("run id may only contain letters, digits, '-' and '_'")

    cfg = ENVIRONMENTS[args.env]
    athena = Athena(cfg["workgroup"], args.region)
    db = cfg["database"]
    failures = 0

    print(f"Run {args.run_id} in {db}")
    for table in sorted({t.table_name: t for t in TABLE_REGISTRY.values()}.values(), key=lambda t: t.table_name):
        keys = ", ".join(table.primary_keys)
        try:
            loaded = athena.scalar_rows(f"select count(*) from {db}.{table.table_name} where run_id = '{args.run_id}'")[0][0]
            dupes = athena.scalar_rows(
                f"select count(*) from (select {keys} from {db}.{table.table_name} group by {keys} having count(*) > 1)"
            )[0][0]
        except RuntimeError as exc:
            print(f"  {table.table_name:<28} ERROR {str(exc).splitlines()[0]}")
            failures += 1
            continue
        ok = dupes == "0"
        failures += 0 if ok else 1
        print(f"  {table.table_name:<28} rows from this run={loaded:>8}  duplicate keys={dupes}  {'OK' if ok else 'FAIL'}")

    if args.dbt:
        rows = athena.scalar_rows(f"""
            select
              (select round(sum(net_total_amount), 2) from {db}.fact_order where is_revenue_recognized = 1),
              (select round(sum(total_net_revenue), 2) from {db}.customer_360),
              (select round(enterprise_net_revenue, 2) from {db}.mart_executive_kpis)
        """)
        fact, c360, kpi = (float(v or 0) for v in rows[0])
        ok = abs(fact - c360) <= 1.0 and abs(fact - kpi) <= 0.01
        failures += 0 if ok else 1
        print(f"  revenue fact_order={fact} customer_360={c360} executive_kpis={kpi}  {'OK' if ok else 'FAIL'}")

    print("RESULT:", "PASS" if failures == 0 else f"FAIL ({failures} checks)")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
