"""CRPD platform parser regressions — real-world document fixtures.

Covers: GBK/GB2312 encoded HTML (Chinese government sites), wrapper/noise
pages, relative attachment URLs, empty attachment bodies, and scanned (blank)
PDFs. All fixtures are deterministic bytes; no network.
"""
from __future__ import annotations

import fitz

from policydb.crawl.parser import parse_document

GBK_PAGE = (
    "<html><head><meta charset='gb2312'><title>关于优化住房公积金贷款政策的通知"
    "</title></head><body><p>自2022年1月1日起，首套住房首付比例调整为20%。"
    "</p></body></html>"
).encode("gbk")


def test_gbk_html_decodes_without_mojibake():
    parsed = parse_document(GBK_PAGE, "text/html; charset=gb2312")
    assert parsed["document_type"] == "html"
    assert "首付比例" in parsed["full_text"]
    assert "关于优化住房公积金贷款政策" in parsed["title"]
    assert parsed["parse_status"] in {"parsed", "partial"}


def test_utf8_html_unchanged_by_charset_detection():
    page = "<html><head><meta charset='utf-8'><title>通知</title></head><body><p>正文内容</p></body></html>"
    parsed = parse_document(page.encode("utf-8"), "text/html")
    assert "正文内容" in parsed["full_text"]
    assert parsed["title"] == "通知"


def test_wrapper_html_strips_nav_footer_noise():
    page = (
        "<html><head><title>通知</title></head><body>"
        "<nav>首页 关于我们 联系我们 登录</nav>"
        "<header>网站标识 无障碍浏览</header>"
        "<main><p>本通知自发布之日起施行。</p><p>各有关单位要认真执行。</p></main>"
        "<footer>ICP备123456号 版权所有 技术支持</footer>"
        "</body></html>"
    )
    parsed = parse_document(page.encode("utf-8"), "text/html")
    assert "本通知自发布之日起施行" in parsed["full_text"]
    assert "ICP备123456号" not in parsed["full_text"]
    assert "网站标识" not in parsed["full_text"]


def test_relative_attachment_url_resolved_against_base():
    page = (
        "<html><head><title>附件通知</title></head><body>"
        "<p>正文</p>"
        "<a href='../attachments/2022/01/tz2022-1.pdf'>附件：通知原文</a>"
        "</body></html>"
    )
    parsed = parse_document(
        page.encode("utf-8"),
        "text/html",
        base_url="https://www.zjj.example.gov.cn/zcfg/2022/01/t20220101.html",
    )
    assert parsed["attachments"], "expected at least one attachment"
    attachment = parsed["attachments"][0]
    assert attachment["source"] == "html_link"
    assert attachment["url"].startswith("https://")
    assert attachment["url"].endswith("attachments/2022/01/tz2022-1.pdf")


def test_empty_body_yields_partial_without_attachments():
    parsed = parse_document(b"", "text/html")
    assert parsed["parse_status"] == "partial"
    assert parsed["full_text"] == ""
    assert parsed["attachments"] == []


def test_scanned_pdf_without_text_layer_is_partial_not_error():
    document = fitz.open()
    document.new_page()  # image-only page: no text layer
    pdf_bytes = document.tobytes()
    parsed = parse_document(pdf_bytes, "application/pdf")
    assert parsed["document_type"] == "pdf"
    assert parsed["page_count"] == 1
    assert parsed["parse_status"] == "partial"
    assert parsed["full_text"] == ""
    assert "parser_error" not in parsed
