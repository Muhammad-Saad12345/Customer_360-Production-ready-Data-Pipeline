"""
Lambda handler: notify  (state NotifyWarnings)

Input:  {"run_id", "environment", "severity", "failed_sources", "warnings", "execution_arn"}
Output: {"sns_message_id"}

Run failures are emailed by the EventBridge rule on the execution status; this
handler only reports runs that continued with warnings. A publish error
raises, and the state machine's Catch continues to dbt (warnings must not
block the transform).
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: Any, sns_client: Any = None) -> Dict[str, Any]:
    topic_arn = os.environ.get("C360_SNS_ALERT_TOPIC_ARN", "")
    environment = event.get("environment", os.environ.get("ENVIRONMENT", "unknown"))
    severity = event.get("severity", "WARNING")
    run_id = event.get("run_id", "unknown")
    if not topic_arn:
        logger.warning("C360_SNS_ALERT_TOPIC_ARN not set; nothing published for run %s", run_id)
        return {"sns_message_id": ""}

    failed = event.get("failed_sources") or []
    warnings = event.get("warnings") or []
    subject = f"[Customer360][{environment}][{severity}] run {run_id}"[:100]
    lines = [
        f"Environment: {environment}",
        f"Run: {run_id}",
        f"Execution: {event.get('execution_arn', '')}",
        f"Failed (non-mandatory) sources: {', '.join(failed) or 'none'}",
        "Warnings:",
        *[f"  - {w}" for w in warnings],
        "",
        "The run continued to dbt. See docs/PRODUCTION_DEPLOYMENT_RUNBOOK.md for triage.",
    ]
    if sns_client is None:
        import boto3

        sns_client = boto3.client("sns")
    response = sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message="\n".join(lines),
        MessageAttributes={"severity": {"DataType": "String", "StringValue": severity}},
    )
    logger.info("Published alert %s: %s", response.get("MessageId"), json.dumps({"failed": failed, "warnings": len(warnings)}))
    return {"sns_message_id": response.get("MessageId", "")}
