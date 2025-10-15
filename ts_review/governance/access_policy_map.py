public_tag = "public.*"
non_pii_tag = "non_pii.*"

access_policy_map = {
    "legal": ["pii.name", "pii.email", "pii.ip_address", "pii.business_name"],
    "data_engineering": [non_pii_tag, public_tag, "system.*"],
    "data_science": [non_pii_tag, public_tag],
    "marketing": [public_tag, "non_pii.rating"],
    "analytics": [public_tag, "non_pii.*e"],
}
