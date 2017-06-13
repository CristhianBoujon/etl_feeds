import configparser
import os

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'config.ini'))

# Import smtplib for the actual sending function
import smtplib

# Import the email modules we'll need
from email.mime.text import MIMEText

def send_email(subject, content):

    email_host = config["email"]["host"]
    email_user = config["email"].get("user")
    email_password = config["email"].get("password")

    # instantiate a SMTP class
    email_server = smtplib.SMTP(email_host)

    # If a user and password is provided, we need to login
    if(email_user and email_password):
        email_server.login(email_user, email_password)


    msg = MIMEText(content)

    msg['Subject'] = subject
    msg['From'] = config["email.notification"]["from"]
    msg['To'] = config["email.notification"]["to"]

    email_server.send_message(msg)
