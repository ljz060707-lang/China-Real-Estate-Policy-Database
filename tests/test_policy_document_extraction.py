from policydb.archive import _archive_folder


def test_document_content_types_have_separate_archive_folders():
    assert _archive_folder("application/pdf", ".pdf") == "pdf"
    assert _archive_folder("text/html", ".html") == "html"
    assert _archive_folder("application/zip", ".zip") == "attachments"
