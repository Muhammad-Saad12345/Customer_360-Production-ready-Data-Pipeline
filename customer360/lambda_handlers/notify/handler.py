"""
Lambda handler: notify
Step Functions state: NotifyFailure / NotifySuccess

Publishes pipeline status alerts to Amazon SNS and emits CloudWatch outcome metrics.
Handles Catch blocks from any failed state in the Step Functions execution.

Input event (from Step Functions Catch block or terminal state):
{
    "run_id": "c360-...",
    "status": "failed",
    "error": {
        "Error": "States.TaskFailed",
        "Cause": "..."
    },
    "failed_state": "Ingest_hubspot",
    "execution_id": "arn:aws:states:us-east-1:734143568343:execution:c360-batch:...",
    "environment": "staging"
}

Output:
{
    "notification_sent": true,
    "sns_message_id": "...",
    "status": "completed"
}
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_SNS_TOPIC_ARN = os.environ.get("C360_SNS_ALERT_TOPIC_ARN")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for the Notify Lambda."""
    logger.info("Notify event received: %s", event)

    run_id: str = event.get("run_id", "unknown-run")
    environment: str = event.get("environment", os.environ.get("ENVIRONMENT", "staging"))
    status: str = event.get("status", "failed").upper()
    failed_state: str = event.get("failed_state", event.get("FailedState", "UnknownState"))
    error_info = event.get("error", event.get("Error", {}))
    execution_id: str = event.get("execution_id", event.get("ExecutionArn", "unknown-arn"))

    cause = ""
    if isinstance(error_info, dict):
        cause = error_info.get("Cause", error_info.get("Error", str(error_info)))
    elif error_info:
        cause = str(error_info)

    subject = f"[{environment.upper()}] Customer 360 Pipeline {status}: {failed_state}"
    subject = subject[:100]

    message_payload = {
        "pipeline": "Customer360-Batch",
        "environment": environment,
        "run_id": run_id,
        "status": status,
        "failed_state": failed_state,
        "cause": cause,
        "execution_arn": execution_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    message_text = json.dumps(message_payload, indent=2)

    topic_arn = os.environ.get("C360_SNS_ALERT_TOPIC_ARN") or _SNS_TOPIC_ARN
    notification_sent = False
    message_id = None

    if topic_arn:
        try:
            import boto3
            region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
            sns = boto3.client("sns", region_name=region)
            resp = sns.publish(
                TopicArn=topic_arn,
                Subject=subject,
                Message=message_text,
            )
            message_id = resp.get("MessageId")
            notification_sent = True
            logger.info("Published SNS notification message_id=%s to %s", message_id, topic_arn)
        except Exception as exc:
            logger.warning("Failed to publish SNS alert (non-fatal): %s", exc)
    else:
        logger.info("No SNS topic configured (C360_SNS_ALERT_TOPIC_ARN unset) — skipped SNS publish")

    # Emit CloudWatch task failure metric if status is failed
    if status == "FAILED":
        try:
            from src.monitoring.cloudwatch import emit_task_outcome
            emit_task_outcome(
                source="pipeline",
                entity="all",
                tier="orchestration",
                status="failed",
                run_id=run_id,
            )
        except Exception as cw_exc:
            logger.warning("Could not emit CloudWatch metric (non-fatal): %s", cw_exc)

    return {
        "notification_sent": notification_sent,
        "sns_message_id": message_id,
        "status": "completed",
        "run_id": run_id,
    }
