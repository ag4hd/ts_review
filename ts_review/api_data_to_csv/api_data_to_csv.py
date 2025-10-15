from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import Dict
from config import Config

from ts_review.governance.access_policy_map import access_policy_map
from ts_review.governance.data_policy_tags import data_policy_tags
from ts_review.governance.pii_masking_rules import (
    hash_sha256,
    partial_mask_ip,
    redact,
    masking_rules_by_tag
)


def apply_governance_tags(df, role: str = "analytics"):
    """
    Dynamically applies PII masking/redaction rules based on column policy tags.
    Uses definitions from:
      - data_policy_tags.data_policy_tags
      - pii_masking_rules.masking_rules_by_tag
    """
    masking_functions = {
        "hash_sha256": hash_sha256,
        "partial_mask": partial_mask_ip,
        "redact": redact
    }

    valid_roles = {"analytics", "data_science", "data_engineering", "legal"}
    if role not in valid_roles:
        raise ValueError(f"Invalid role '{role}'. Must be one of {valid_roles}.")

    allowed_tags = access_policy_map.get(role, [])

    for col_name, tag in data_policy_tags.items():
        if tag in masking_rules_by_tag and tag not in allowed_tags:
            rule_name = masking_rules_by_tag[tag]
            mask_func = masking_functions.get(rule_name)
            if mask_func:
                df = df.withColumn(col_name, mask_func(col_name))
    return df


def write_df_to_csv(
        df: DataFrame,
        output_path: str,
        coalesce_one: bool = True
) -> None:
    writer = df.write.mode("overwrite").option("header", "true")
    if coalesce_one:
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    else:
        writer.csv(output_path)

