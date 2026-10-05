"""Tests for the CloudWatch metrics emitter (Phase 6).

These tests run fully offline — no actual AWS calls are made.
The emitter must degrade gracefully when boto3 / credentials are absent.
"""
from __future__ import annotations

import importlib
import os
import sys
import types
import unittest
from unittest.mock import MagicMock, call, patch


# ---------------------------------------------------------------------------
# Helper: reload cloudwatch module under a controlled environment
# ---------------------------------------------------------------------------

def _reload_cw(env: str = "test", topic_arn: str | None = None):
    """Reload the cloudwatch module with a controlled ENVIRONMENT variable."""
    env_patch = {"ENVIRONMENT": env}
    if topic_arn:
        env_patch["C360_SNS_ALERT_TOPIC_ARN"] = topic_arn
    with patch.dict(os.environ, env_patch, clear=False):
        mod = importlib.import_module("src.monitoring.cloudwatch")
        importlib.reload(mod)
        return mod


class TestCloudWatchNoop(unittest.TestCase):
    """Emitter must be a no-op when ENVIRONMENT=test."""

    def setUp(self):
        self.cw = _reload_cw(env="test")

    def test_emit_dq_metrics_noop_in_test(self):
        """emit_dq_metrics must not raise even with no AWS credentials."""
        # Should complete without exception
        self.cw.emit_dq_metrics(
            source="hubspot",
            entity="contacts",
            input_count=100,
            valid_count=98,
            invalid_count=2,
            tier="quality",
        )

    def test_emit_task_outcome_noop_in_test(self):
        self.cw.emit_task_outcome(
            source="oms",
            entity="orders",
            tier="ingest",
            success=True,
            duration_seconds=12.5,
        )

    def test_emit_dq_gate_verdict_noop_in_test(self):
        self.cw.emit_dq_gate_verdict(
            source="pim",
            entity="products",
            passed=False,
            rejection_rate_pct=15.5,
        )

    def test_emit_pipeline_health_noop_in_test(self):
        self.cw.emit_pipeline_health(overall_status="ok", error_count=0, warning_count=1)


class TestCloudWatchPutCalled(unittest.TestCase):
    """When a real boto3 client is injected, put_metric_data must be called."""

    def _make_mock_client(self):
        client = MagicMock()
        client.put_metric_data.return_value = {}
        return client

    def test_emit_dq_metrics_calls_put(self):
        cw = _reload_cw(env="dev")
        mock_client = self._make_mock_client()
        # Inject mock client directly
        cw._cw_client = mock_client
        cw.emit_dq_metrics(
            source="hubspot",
            entity="contacts",
            input_count=200,
            valid_count=190,
            invalid_count=10,
        )
        self.assertTrue(mock_client.put_metric_data.called)
        # Verify namespace is correct
        call_kwargs = mock_client.put_metric_data.call_args[1]
        self.assertEqual(call_kwargs["Namespace"], "Customer360/Pipeline")

    def test_emit_dq_metrics_rejection_rate_value(self):
        """Rejection rate metric value must be 5.0 for 10 invalid out of 200."""
        cw = _reload_cw(env="dev")
        mock_client = self._make_mock_client()
        cw._cw_client = mock_client
        cw.emit_dq_metrics(
            source="oms",
            entity="orders",
            input_count=200,
            valid_count=190,
            invalid_count=10,
        )
        all_metric_data = []
        for c in mock_client.put_metric_data.call_args_list:
            all_metric_data.extend(c[1]["MetricData"])
        rejection_metric = next(m for m in all_metric_data if m["MetricName"] == "RejectionRate")
        self.assertAlmostEqual(rejection_metric["Value"], 5.0)

    def test_emit_task_outcome_success_metric_name(self):
        cw = _reload_cw(env="dev")
        mock_client = self._make_mock_client()
        cw._cw_client = mock_client
        cw.emit_task_outcome(
            source="pim", entity="products", tier="ingest", success=True, duration_seconds=30.0
        )
        all_names = [
            m["MetricName"]
            for c in mock_client.put_metric_data.call_args_list
            for m in c[1]["MetricData"]
        ]
        self.assertIn("TaskSuccess", all_names)
        self.assertNotIn("TaskFailure", all_names)

    def test_emit_task_outcome_failure_metric_name(self):
        cw = _reload_cw(env="dev")
        mock_client = self._make_mock_client()
        cw._cw_client = mock_client
        cw.emit_task_outcome(
            source="loyalty", entity="loyalty", tier="iceberg", success=False
        )
        all_names = [
            m["MetricName"]
            for c in mock_client.put_metric_data.call_args_list
            for m in c[1]["MetricData"]
        ]
        self.assertIn("TaskFailure", all_names)
        self.assertNotIn("TaskSuccess", all_names)

    def test_emit_dq_gate_passed(self):
        cw = _reload_cw(env="dev")
        mock_client = self._make_mock_client()
        cw._cw_client = mock_client
        cw.emit_dq_gate_verdict(source="oms", entity="orders", passed=True, rejection_rate_pct=2.0)
        all_names = [
            m["MetricName"]
            for c in mock_client.put_metric_data.call_args_list
            for m in c[1]["MetricData"]
        ]
        self.assertIn("DQGatePassed", all_names)
        self.assertNotIn("DQGateFailed", all_names)

    def test_emit_dq_gate_failed(self):
        cw = _reload_cw(env="dev")
        mock_client = self._make_mock_client()
        cw._cw_client = mock_client
        cw.emit_dq_gate_verdict(source="oms", entity="orders", passed=False, rejection_rate_pct=25.0)
        all_names = [
            m["MetricName"]
            for c in mock_client.put_metric_data.call_args_list
            for m in c[1]["MetricData"]
        ]
        self.assertIn("DQGateFailed", all_names)

    def test_put_failure_does_not_raise(self):
        """A boto3 exception during put_metric_data must not propagate."""
        cw = _reload_cw(env="dev")
        mock_client = self._make_mock_client()
        mock_client.put_metric_data.side_effect = Exception("AWS throttling")
        cw._cw_client = mock_client
        # Must not raise
        cw.emit_dq_metrics(
            source="support", entity="tickets", input_count=50, valid_count=50, invalid_count=0
        )

    def test_dimensions_include_environment(self):
        cw = _reload_cw(env="prod")
        mock_client = self._make_mock_client()
        cw._cw_client = mock_client
        cw.emit_task_outcome(source="hubspot", entity="contacts", tier="silver", success=True)
        all_dims = [
            dim
            for c in mock_client.put_metric_data.call_args_list
            for m in c[1]["MetricData"]
            for dim in m["Dimensions"]
        ]
        env_dims = [d for d in all_dims if d["Name"] == "Environment"]
        self.assertTrue(any(d["Value"] == "prod" for d in env_dims))


class TestRejectionRatePolicy(unittest.TestCase):
    """Unit tests for the _quality_gate rejection-rate enforcement in the DAG.

    Because importing the DAG directly requires Airflow, we test the policy
    logic in isolation using a thin replica of the _quality_gate guard.
    """

    @staticmethod
    def _apply_policy(
        input_count: int,
        invalid_count: int,
        hard_pct: float = 10.0,
        warn_pct: float = 5.0,
    ) -> tuple[bool, float]:
        """Returns (blocked: bool, rejection_rate_pct: float)."""
        rejection_rate_pct = (invalid_count / input_count * 100.0) if input_count else 0.0
        if rejection_rate_pct >= hard_pct:
            return True, rejection_rate_pct
        return False, rejection_rate_pct

    def test_below_warn_threshold_passes(self):
        blocked, rate = self._apply_policy(100, 3)  # 3%
        self.assertFalse(blocked)
        self.assertAlmostEqual(rate, 3.0)

    def test_between_warn_and_hard_does_not_block(self):
        blocked, rate = self._apply_policy(100, 7)  # 7% — above warn, below hard
        self.assertFalse(blocked)
        self.assertAlmostEqual(rate, 7.0)

    def test_at_hard_threshold_blocks(self):
        blocked, rate = self._apply_policy(100, 10)  # exactly 10%
        self.assertTrue(blocked)
        self.assertAlmostEqual(rate, 10.0)

    def test_above_hard_threshold_blocks(self):
        blocked, rate = self._apply_policy(100, 25)  # 25%
        self.assertTrue(blocked)

    def test_zero_input_zero_rate(self):
        blocked, rate = self._apply_policy(0, 0)
        self.assertFalse(blocked)
        self.assertEqual(rate, 0.0)

    def test_custom_hard_threshold(self):
        # With a relaxed 20% threshold, 15 invalid out of 100 should pass
        blocked, rate = self._apply_policy(100, 15, hard_pct=20.0)
        self.assertFalse(blocked)
        self.assertAlmostEqual(rate, 15.0)

    def test_100_percent_rejection_blocks(self):
        blocked, rate = self._apply_policy(50, 50)
        self.assertTrue(blocked)
        self.assertAlmostEqual(rate, 100.0)


if __name__ == "__main__":
    unittest.main()
