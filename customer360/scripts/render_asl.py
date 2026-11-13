"""Render the state machine template with placeholder ARNs for offline validation.

Checks that the ${...} variables in the ASL template and the templatefile()
call in terraform/modules/step_functions/main.tf are the same set, then writes
build/asl_rendered.json for `npx asl-validator --json-path build/asl_rendered.json`.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "step_functions" / "customer360_batch.asl.json"
MODULE = ROOT / "terraform" / "modules" / "step_functions" / "main.tf"
OUT = ROOT / "build" / "asl_rendered.json"


def main() -> int:
    text = TEMPLATE.read_text()
    asl_vars = set(re.findall(r"\$\{([a-z0-9_]+)\}", text))
    tf = MODULE.read_text()
    block = tf[tf.index("templatefile("):]
    block = block[: block.index("})")]
    tf_vars = set(re.findall(r"^\s*([a-z0-9_]+)\s*=", block, re.M))
    if asl_vars != tf_vars:
        print(f"Template variables differ. ASL only: {asl_vars - tf_vars}  Terraform only: {tf_vars - asl_vars}")
        return 1
    for name in asl_vars:
        if name.endswith("_lambda_arn"):
            value = f"arn:aws:lambda:us-east-1:123456789012:function:c360-ci-{name[:-11].replace('_', '-')}"
        elif name == "glue_dbt_job_name":
            value = "c360-ci-dbt-runner"
        else:
            value = "s3://c360-ci/artifacts/ci/dbt/dbt_project.tar.gz"
        text = text.replace("${" + name + "}", value)
    json.loads(text)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text)
    print(f"Rendered {len(asl_vars)} template variables -> {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
