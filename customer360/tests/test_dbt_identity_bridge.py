"""Runs the identity bridge SQL on DuckDB with a small fixture.

DuckDB is not Athena, but the bridge only uses portable SQL (joins, min,
group by, case), so this catches logic errors such as one source key fanning
out into several master customer ids, which would double-count revenue.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

BRIDGE = Path(__file__).resolve().parent.parent / "dbt" / "models" / "identity" / "bridge_customer_identity.sql"


def _bridge_sql() -> str:
    sql = re.sub(r"\{\{ config\(.*?\) \}\}", "", BRIDGE.read_text(), flags=re.S)
    return (sql.replace("{{ ref('stg_hubspot_contacts') }}", "stg_hubspot_contacts")
               .replace("{{ ref('int_customer_identities_unioned') }}", "ids"))


@pytest.fixture()
def con():
    c = duckdb.connect()
    c.execute("create table stg_hubspot_contacts(hubspot_contact_id varchar, email varchar, phone varchar)")
    c.execute("""insert into stg_hubspot_contacts values
        ('9001', 'a@x.com', '+15550001'), ('9002', 'b@x.com', null), ('9003', 'b@x.com', null)""")
    c.execute("create table ids(source_system varchar, source_customer_id varchar, email varchar, phone varchar)")
    c.execute("""insert into ids values
        ('hubspot', '9001', 'a@x.com', '+15550001'), ('hubspot', '9002', 'b@x.com', null), ('hubspot', '9003', 'b@x.com', null),
        ('oms', 'C-1', 'A@X.com', null), ('oms', 'C-1', 'a@x.com', null), ('loyalty', 'C-1', null, null),
        ('oms', 'C-2', 'b@x.com', null), ('zendesk', 'C-3', null, '+15550001'),
        ('marketing', 'C-9', 'nobody@x.com', null), ('loyalty', '9002', null, null)""")
    return c


def test_each_source_key_resolves_once(con):
    dups = con.execute(f"select source_system, source_customer_id from ({_bridge_sql()}) group by 1, 2 having count(*) > 1").fetchall()
    assert dups == []


def test_rules(con):
    rows = {(r[1], r[2]): (r[0], r[3]) for r in con.execute(_bridge_sql()).fetchall()}
    assert rows[("oms", "C-1")] == ("MCID_9001", "exact_email_match")              # case-insensitive email
    assert rows[("loyalty", "C-1")] == ("MCID_9001", "shared_customer_id_email_match")  # via the OMS order email
    assert rows[("oms", "C-2")] == ("MCID_9002", "exact_email_match")              # shared email -> lowest contact id
    assert rows[("zendesk", "C-3")] == ("MCID_9001", "exact_phone_match")
    assert rows[("loyalty", "9002")] == ("MCID_9002", "authoritative_id_match")
    assert rows[("marketing", "C-9")] == ("MCID_MARKETING_C-9", "unresolved_singleton_exception")
