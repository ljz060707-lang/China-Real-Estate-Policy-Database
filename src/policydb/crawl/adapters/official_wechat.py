from policydb.crawl.adapters.generic_government import GenericGovernmentAdapter


class OfficialWechatAdapter(GenericGovernmentAdapter):
    """Discovery-only adapter; official status must be verified against an agency account."""

