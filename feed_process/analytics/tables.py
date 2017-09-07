from sqlalchemy import *
from sqlalchemy import Column, Integer, String, Float, TIMESTAMP, Unicode, UnicodeText, ForeignKey, Boolean, Date


metadata = MetaData()

# Fact table
fact_processed_ads = Table("fact_processed_ads", metadata,
    Column("id", primary_key = True),
    Column("category_id", Integer, ForeignKey("dim_category.id")),
    Column("country_id", Integer, ForeignKey("dim_country.id")),
    Column("date_id", Integer, ForeignKey("dim_date.id")),
    Column("feed_in_id", Integer, ForeignKey("dim_feed_in.id")),
    Column("entered_ads", Integer),
    Column("loaded_ads", Integer),
    Column("errors", Integer),
    Column("entered_imgs", Integer),
    Column("loaded_imgs", Integer))

# Dimesional tables
dim_category = Table("dim_category", metadata,
    Column("id", Integer, primary_key = True),
    Column("name", String))

dim_feed = Table("dim_feed_in", metadata,
    Column("id", Integer, primary_key = True),
    Column("name", String))

dim_country = Table("dim_country", metadata,
    Column("id", Integer, primary_key = True),
    Column("name", String))

dim_date = Table("dim_date", metadata,
    Column("id", Integer, primary_key = True),
    Column("date", Date))
