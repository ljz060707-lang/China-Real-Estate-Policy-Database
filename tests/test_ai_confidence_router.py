from policydb.confidence import review_required


def test_ai_confidence_router_requires_conflicts_to_be_reviewed():
    assert not review_required(0.90, conflict=False, official=True)
    assert review_required(0.99, conflict=True, official=True)
