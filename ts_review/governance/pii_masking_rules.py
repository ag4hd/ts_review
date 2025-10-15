from pyspark.sql.functions import col, sha2, lit, regexp_replace


def hash_sha256(col_name):
    return sha2(col(col_name), 256)


def partial_mask_ip(col_name):
    return regexp_replace(col(col_name), r'\d{1,3}$', '***')


def redact(col_name):
    return lit("[REDACTED]")


masking_rules_by_tag = {
    "pii.email": "hash_sha256",
    "pii.ip_address": "partial_mask",
    "pii.name": "redact",
    "pii.business_name": "redact"
}

