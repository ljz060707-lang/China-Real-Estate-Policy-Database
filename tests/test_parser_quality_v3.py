from io import BytesIO
from zipfile import ZipFile

import pytest

from policydb.crawl.parser import parse_document


@pytest.mark.parametrize("body", [
    b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" + b"\0binary" * 50,
    b"Rar!\x1a\x07\x00" + b"binary" * 50,
    b"MZ" + b"\0binary" * 50,
    b"PK\x03\x04" + b"broken zip" * 50,
])
def test_binary_is_never_decoded_as_html(body):
    result = parse_document(body, "text/html")
    assert result["full_text"] == ""
    assert result["parse_status"] in {"deferred_unsupported", "parser_error"}
    assert result["document_type"] != "html"


def test_office_magic_overrides_missing_mime():
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        archive.writestr("word/document.xml", '<w:document xmlns:w="urn:word"><w:p><w:r><w:t>原文正文</w:t></w:r></w:p></w:document>')
    result = parse_document(output.getvalue(), "application/octet-stream")
    assert result["document_type"] == "docx"
    assert result["full_text"] == "原文正文"


def test_generic_archive_is_deferred_not_a_policy():
    output = BytesIO()
    with ZipFile(output, "w") as archive:
        archive.writestr("software.exe", b"MZ\0\0")
    result = parse_document(output.getvalue(), None)
    assert result["document_type"] == "zip"
    assert result["parse_status"] == "deferred_unsupported"
    assert result["full_text"] == ""


def test_specific_body_wins_over_larger_outer_container():
    html = '<title>通知</title><div class="content"><div class="related"><p>' + '不相关导航' * 100 + '</p></div><div class="TRS_Editor"><p>第一条，保留完整政策原文。</p><p>第二条，本通知自发布之日起实施。</p></div></div>'
    result = parse_document(html.encode(), "text/html")
    assert "不相关导航" not in result["full_text"]
    assert "第二条" in result["full_text"]


def test_direct_dom_text_is_not_lost_when_paragraph_also_exists():
    result = parse_document('<article>第一条：不得遗漏此段。<p>第二条：保留段落。</p><table><tr><td><p>比例20%</p></td><td>期限5年</td></tr></table>落款日期：2024年1月1日。</article>'.encode(), "text/html")
    assert "不得遗漏此段" in result["full_text"]
    assert result["full_text"].count("比例20%") == 1
    assert "2024年1月1日" in result["full_text"]


def test_soft404_not_parsed_despite_boilerplate_length():
    result = parse_document(('<title>页面不存在</title><body>抱歉，您访问的页面不存在。' + '网站导航栏目' * 100 + '</body>').encode(), "text/html")
    assert result["parse_status"] == "soft_error"
    assert result["full_text"] == ""


def test_short_real_notice_is_retained_without_fabricating_completion():
    result = parse_document('<article>本通知自发布之日起施行。</article>'.encode(), "text/html")
    assert result["full_text"] == "本通知自发布之日起施行。"
    assert result["parse_status"] == "partial"
