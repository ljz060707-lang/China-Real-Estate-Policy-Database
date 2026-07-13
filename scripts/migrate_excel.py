import sys

from policydb.ingest.excel import import_excel

print(import_excel(sys.argv[1]))
