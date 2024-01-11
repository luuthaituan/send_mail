import json
import requests
from prettytable import PrettyTable
import pandas as pd
import pymysql
import sshtunnel
import telegram
import asyncio


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


async def send_table_to_telegram(result, headers, config):
    if result.empty:
        message = "No data in the table."
        print(message)
    else:
        headers_list = list(headers)

        table = PrettyTable(headers_list)
        for _, row in result.iterrows():
            table.add_row(row.tolist())

        message = f"Data in the table:\n\n{table.get_string()}\n"

        # Initialize the Telegram bot
        bot = telegram.Bot(token=config["telegram"]["bot_token"])

        # Send the message to the specified chat ID
        await bot.send_message(chat_id=config["telegram"]["chat_id"], text=message)


async def main():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    tunnel = open_ssh_tunnel(config)
    connection = mysql_connect(config, tunnel)

    try:
        df = run_query(config["query"], connection)
        df.head()
        print(df.head())

        await send_table_to_telegram(df, df.columns, config)

    finally:
        mysql_disconnect(connection)
        close_ssh_tunnel(tunnel)


if __name__ == "__main__":
    asyncio.run(main())