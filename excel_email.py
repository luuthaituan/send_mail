import json
import requests
from prettytable import PrettyTable
import pandas as pd
import pymysql
import sshtunnel
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


def open_ssh_tunnel(config, verbose=False):
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = sshtunnel.logging.DEBUG

    tunnel = sshtunnel.SSHTunnelForwarder(
        ssh_address=(config["ssh"]["host"], config["ssh"]["port"]),
        ssh_username=config["ssh"]["username"],
        ssh_password=config["ssh"]["password"],
        remote_bind_address=('127.0.0.1', 3306)
    )

    tunnel.start()
    return tunnel


def mysql_connect(config, tunnel):
    connection = pymysql.connect(
        host='127.0.0.1',
        user=config["mysql"]["user"],
        passwd=config["mysql"]["password"],
        db=config["mysql"]["db"],
        port=tunnel.local_bind_port
    )
    return connection


def run_query(sql, connection):
    return pd.read_sql_query(sql, connection)


def mysql_disconnect(connection):
    connection.close()


def close_ssh_tunnel(tunnel):
    tunnel.close()


def send_table_to_email(result, headers, config):
    if result.empty:
        message = "No data in the table."
        print(message)
    else:
        headers_list = list(headers)

        # Create a Pandas Excel writer using XlsxWriter as the engine
        excel_file = "table_data.xlsx"
        writer = pd.ExcelWriter(excel_file, engine="xlsxwriter")

        # Convert the DataFrame to an Excel object
        result.to_excel(writer, index=False, sheet_name="Table Data")

        # Close the Pandas Excel writer
        writer.close()

        # Create the email
        email = MIMEMultipart()
        email['From'] = config["email"]["sender"]
        email['To'] = config["email"]["recipient"]
        email['Subject'] = "Table Data"

        # Add a text body to the email
        email.attach(MIMEText("Please find the attached table data.", "plain"))

        # Attach the Excel file to the email
        with open(excel_file, "rb") as file:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition", f"attachment; filename={excel_file}"
            )
            email.attach(part)

        # Connect to the SMTP server
        with smtplib.SMTP(config["email"]["smtp_server"], config["email"]["smtp_port"]) as smtp:
            smtp.starttls()
            smtp.login(config["email"]["username"], config["email"]["password"])
            smtp.send_message(email)


if __name__ == "__main__":
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    tunnel = open_ssh_tunnel(config)
    connection = mysql_connect(config, tunnel)

    try:
        df = run_query(config["query"], connection)
        df.head()
        print(df.head())

        send_table_to_email(df, df.columns, config)

    finally:
        mysql_disconnect(connection)
        close_ssh_tunnel(tunnel)