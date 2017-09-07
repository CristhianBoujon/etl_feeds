from sqlalchemy import func
from sqlalchemy.sql.expression import select, between
from feed_process.analytics.tables import *
import datetime as dtt
from feed_process.analytics.db import engine
from matplotlib import cm
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def query_builder(dimension, limit = None):
    reference_date = dtt.date.today()
    last_week = reference_date - dtt.timedelta(7)
    last_month = dtt.date(reference_date.year, reference_date.month - 1, reference_date.day)

    reference_date_id = reference_date.strftime("%Y%m%d")
    last_week_id = last_week.strftime("%Y%m%d")
    last_month_id = last_month.strftime("%Y%m%d")

    return select([
                dimension.c.name,
                func.sum(
                    func.IF(
                        between(fact_processed_ads.c.date_id, last_week_id, reference_date_id), 
                        fact_processed_ads.c.loaded_ads, 0)),
                
                func.sum(
                    func.IF(
                        between(fact_processed_ads.c.date_id, last_month_id, reference_date_id), 
                        fact_processed_ads.c.loaded_ads, 0))
                ], group_by = dimension.c.name).\
                select_from(fact_processed_ads.join(dimension)).\
                limit(limit)


def get_resume(dimension, limit = None):
    dimension_query = query_builder(dimension)

    result = list(engine.connect().execute(dimension_query))
    df = pd.DataFrame(
            {"by_week": [row[1] for row in result], "by_month": [row[2] for row in result]}, 
            index = [row[0] for row in result])

    df["by_week"] = df["by_week"].astype(np.int64)
    df["by_month"] = df["by_month"].astype(np.int64)

    return df

category_resume = get_resume(dim_category)

category_plot = category_resume.plot(
    kind = "pie",
    autopct = '%1.1f%%', 
    startangle = 90,
    subplots=True,
    legend = False,
    title = ["Últimos 30 días", "Últimos 7 días"])

for ax in category_plot:
    ax.set_aspect('equal')

# It saves figure as image
plt.savefig("cat.png")

category_html = category_resume.to_html()

country_resume = get_resume(dim_country)
country_resume.sort_values(["by_week"]).plot(
    kind = "barh", 
    figsize = (15,5),
    layout = (1, 2), 
    subplots = True,
    sharex = False,
    legend = False,
    title = ["Últimos 30 días", "Últimos 7 días"])

# It saves figure as image 
plt.savefig("country.png")

country_html = country_resume.to_html()


################### send by email
import smtplib
# Import the email modules we'll need
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

content = """
    <h1> Ads insertados por Categoría </h1>
    {category_data}

    <br/><br/>

    <h1> Ads insertados por País </h1>
    {country_data}
    """.format(category_data = category_html, country_data = country_html)


email_host = ""
email_user = ""
email_password = ""

# instantiate a SMTP class
email_server = smtplib.SMTP(email_host)

# If a user and password is provided, we need to login
if(email_user and email_password):
    email_server.login(email_user, email_password)


msg = MIMEMultipart()

msg['Subject'] = "Reporte semanal"
msg['From'] = ""
msg['To'] = ""

msg.attach(MIMEImage(open("cat.png", 'rb').read()))
msg.attach(MIMEImage(open("country.png", 'rb').read()))
msg.attach(MIMEText(content, 'html'))

email_server.send_message(msg)