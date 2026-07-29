from policydb.taxonomy_v2 import classify_action


def test_action_classification_stays_within_five_domains():
    primary, secondary, mechanism, confidence, evidence = classify_action(
        "provident_fund", "提高住房公积金贷款额度"
    )
    assert primary in {"D", "S", "F", "H", "G"}
    assert secondary and mechanism and confidence >= 0.9 and evidence
