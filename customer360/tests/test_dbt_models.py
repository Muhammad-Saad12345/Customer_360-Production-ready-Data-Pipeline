from __future__ import annotations

import unittest
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt"
MODELS_DIR = DBT_DIR / "models"


class TestDbtProjectStructure(unittest.TestCase):
    """Verify dbt project configuration, model hierarchy, and architectural boundaries."""

    def test_dbt_project_configuration(self):
        project_file = DBT_DIR / "dbt_project.yml"
        self.assertTrue(project_file.exists(), "dbt_project.yml must exist")
        with open(project_file, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        self.assertEqual(cfg["name"], "customer360_dbt")
        self.assertEqual(cfg["profile"], "customer360")
        self.assertIn("models", cfg)

    def test_sources_declaration_covers_all_active_iceberg_tables(self):
        sources_file = MODELS_DIR / "staging" / "sources.yml"
        self.assertTrue(sources_file.exists(), "staging/sources.yml must exist")
        with open(sources_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        src = data["sources"][0]
        self.assertEqual(src["schema"], "customer360_iceberg")
        table_names = {t["name"] for t in src["tables"]}
        expected_tables = {
            "hubspot_contacts",
            "oms_orders",
            "pim_products",
            "loyalty_loyalty",
            "marketing_campaign_events",
            "zendesk_tickets",
        }
        self.assertEqual(expected_tables, table_names)

    def test_all_staging_models_exist(self):
        expected_staging = [
            "stg_hubspot_contacts.sql",
            "stg_oms_orders.sql",
            "stg_pim_products.sql",
            "stg_loyalty.sql",
            "stg_marketing_campaign_events.sql",
            "stg_zendesk_tickets.sql",
        ]
        for name in expected_staging:
            path = MODELS_DIR / "staging" / name
            self.assertTrue(path.exists(), f"Missing staging model: {name}")
            content = path.read_text(encoding="utf-8")
            self.assertIn("source('iceberg_silver'", content)

    def test_identity_resolution_deterministic_matching(self):
        bridge_file = MODELS_DIR / "identity" / "bridge_customer_identity.sql"
        self.assertTrue(bridge_file.exists(), "bridge_customer_identity.sql must exist")
        sql = bridge_file.read_text(encoding="utf-8")
        self.assertIn("authoritative_crm_primary", sql)
        self.assertIn("exact_email_match", sql)
        self.assertIn("exact_phone_match", sql)
        self.assertIn("unresolved_singleton_exception", sql)
        self.assertIn("rule_version", sql)
        self.assertIn("match_confidence", sql)

    def test_golden_record_explicit_survivorship(self):
        dim_cust_file = MODELS_DIR / "dimensions" / "dim_customer.sql"
        self.assertTrue(dim_cust_file.exists(), "dim_customer.sql must exist")
        sql = dim_cust_file.read_text(encoding="utf-8")
        self.assertIn("SURVIVORSHIP RULES", sql)
        self.assertIn("survivorship_authority_manifest", sql)
        self.assertIn("master_customer_id", sql)
        self.assertIn("crm_profiles", sql)
        self.assertIn("oms_summary", sql)
        self.assertIn("loyalty_ledger", sql)

    def test_customer_360_analytical_mart(self):
        c360_file = MODELS_DIR / "marts" / "customer_360.sql"
        self.assertTrue(c360_file.exists(), "customer_360.sql must exist")
        sql = c360_file.read_text(encoding="utf-8")
        self.assertIn("master_customer_id", sql)
        self.assertIn("rfm_customer_segment", sql)
        self.assertIn("customer_churn_risk", sql)
        self.assertIn("estimated_clv", sql)
        self.assertIn("total_completed_orders", sql)
        self.assertIn("total_net_revenue", sql)

    def test_business_kpi_marts(self):
        metrics_file = MODELS_DIR / "marts" / "mart_customer_metrics.sql"
        exec_file = MODELS_DIR / "marts" / "mart_executive_kpis.sql"
        self.assertTrue(metrics_file.exists(), "mart_customer_metrics.sql must exist")
        self.assertTrue(exec_file.exists(), "mart_executive_kpis.sql must exist")
        metrics_sql = metrics_file.read_text(encoding="utf-8")
        self.assertIn("repeat_purchase_rate_pct", metrics_sql)
        self.assertIn("cohort_aov", metrics_sql)

    def test_data_quality_tests_exist(self):
        singular_tests = [
            "assert_no_negative_order_amounts.sql",
            "assert_no_orphan_orders.sql",
            "assert_bridge_identity_confidence_bounds.sql",
            "assert_no_duplicate_master_customers.sql",
        ]
        for t in singular_tests:
            path = DBT_DIR / "tests" / t
            self.assertTrue(path.exists(), f"Missing singular test: {t}")

        schema_file = MODELS_DIR / "schema.yml"
        self.assertTrue(schema_file.exists(), "schema.yml must exist")
        with open(schema_file, "r", encoding="utf-8") as f:
            schema = yaml.safe_load(f)
        model_names = {m["name"] for m in schema["models"]}
        self.assertIn("customer_360", model_names)
        self.assertIn("dim_customer", model_names)
        self.assertIn("bridge_customer_identity", model_names)

    def test_architectural_boundary_enforcement(self):
        """CRITICAL: dbt must begin at the Iceberg layer. No raw API ingestion or raw validation in dbt."""
        for path in MODELS_DIR.rglob("*.sql"):
            content = path.read_text(encoding="utf-8").lower()
            self.assertNotIn("requests.get", content)
            self.assertNotIn("boto3", content)
            self.assertNotIn("raw/", content)


if __name__ == "__main__":
    unittest.main()
