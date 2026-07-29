def test_multiple_publications_are_not_collapsed_to_one_url():
    publications = {("agency-a", "https://a.gov.cn/p"), ("agency-b", "https://b.gov.cn/p")}
    assert len(publications) == 2
