from policydb import PolicyDB

db = PolicyDB.open()
db.export(db.research.city_month_panel(), "data/research/city_month_panel.parquet")
db.export(db.research.city_year_panel(), "data/research/city_year_panel.parquet")
