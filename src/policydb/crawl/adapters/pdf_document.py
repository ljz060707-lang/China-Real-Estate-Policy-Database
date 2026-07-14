from policydb.crawl.parser import parse_document


class PDFDocumentAdapter:
    def parse(self, body: bytes) -> dict:
        return parse_document(body, "application/pdf")
