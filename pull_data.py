import requests
import json
import time
import sched
from datetime import datetime
from datetime import timedelta
import pymysql.cursors
import pytz
import time

from start_timestamps import start_timestamps
import mysql_config
import granularities


def float_parser(x):
  res = 0
  try:
    res = float(x)
  except:
    res = 0

  return res


def create_table(coin, interval):
  conn = pymysql.connect(host=mysql_config.host,
                         user=mysql_config.user,
                         password=mysql_config.password,
                         db=mysql_config.db,
                         charset=mysql_config.charset,
                         cursorclass=pymysql.cursors.DictCursor)
  try:
    with conn.cursor() as cursor:
      sql = "CREATE TABLE IF NOT EXISTS `price_{}_{}` (`timestamp` bigint(20) unsigned NOT NULL, `datetime` varchar(30) COLLATE utf8mb4_general_ci NOT NULL DEFAULT '', `low`double NOT NULL DEFAULT '0', `high`double NOT NULL DEFAULT '0', `open`double NOT NULL DEFAULT '0', `close`double NOT NULL DEFAULT '0', `volume` double NOT NULL DEFAULT '0', PRIMARY KEY (`timestamp`));".format(
        coin, interval)
      cursor.execute(sql)

      sql = "CREATE TABLE IF NOT EXISTS `up_down_{}_{}` (`timestamp` bigint(20) unsigned NOT NULL, `upPercent` double NOT NULL DEFAULT '0', `upPrice` double NOT NULL DEFAULT '0', `downPercent` double NOT NULL DEFAULT '0', `downPrice` double NOT NULL DEFAULT '0', PRIMARY KEY (`timestamp`));".format(
        coin, interval)
      cursor.execute(sql)

      sql = "CREATE OR REPLACE VIEW `up_down_view_{}_{}` AS (SELECT P.*, D.upPercent, D.upPrice, D.downPercent, D.downPrice FROM `price_{}_{}` P LEFT JOIN `up_down_{}_{}` D ON P.timestamp = D.timestamp);".format(
        coin, interval, coin, interval, coin, interval)
      cursor.execute(sql)
  except Exception as inst:
    print(type(inst), inst.args, inst)
    # print(conn.cursor()._last_executed)
    conn.close()
    return {
      "statusCode": 500,
      "step": "Create Table",
    }

  return {
    "statusCode": 200,
    "step": "Create Table"
  }


def get_last_timestamp(coin, interval):
  conn = pymysql.connect(host=mysql_config.host,
                         user=mysql_config.user,
                         password=mysql_config.password,
                         db=mysql_config.db,
                         charset=mysql_config.charset,
                         cursorclass=pymysql.cursors.DictCursor)
  try:
    with conn.cursor() as cursor:
      sql = "SELECT `timestamp` FROM `price_{}_{}` ORDER BY `timestamp` DESC LIMIT 1;".format(coin, interval)
      cursor.execute(sql)
      row = cursor.fetchone()
      # print(sql, row)
      if row is not None:
        timestamp = datetime.fromtimestamp(row["timestamp"], tz=pytz.utc)
        return timestamp.isoformat()[:19]
  except Exception as inst:
    return start_timestamps[coin]

  return start_timestamps[coin]


def pull_data(coin, granularity, start_time):
  url = "https://api.pro.coinbase.com/products/{}/candles".format(coin)
  timestamp = datetime.fromisoformat(start_time) + timedelta(days=100 if granularity == granularities.day else 0,
                                                             hours=0 if granularity == granularities.day else 100)

  end_time = timestamp.isoformat()[:19]
  params = {
    "start": start_time,
    "end": end_time,
    "granularity": granularity,
  }
  r = requests.get(url=url, params=params)

  formatted_string = ""
  try:
    formatted_string = r.text.replace("'", '"')
    rows = json.loads(formatted_string)
  except Exception as inst:
    print(type(inst), inst.args, inst)
    return {
      "statusCode": 500,
      "step": "Pull Data",

      "formatted_string": formatted_string
    }

  r_cnt = len(rows)
  if r_cnt == 0:
    return {
      "statusCode": 200,
      "step": "Pull Data",
      "body": "OK",
      "coin": coin,
      "granularity": granularity,
      "startTime": start_time
    }

  conn = pymysql.connect(host=mysql_config.host,
                         user=mysql_config.user,
                         password=mysql_config.password,
                         db=mysql_config.db,
                         charset=mysql_config.charset,
                         cursorclass=pymysql.cursors.DictCursor)

  try:
    with conn.cursor() as cursor:
      # Create a new record
      sql1 = "INSERT INTO `price_{}_{}`(`timestamp`, `datetime`, `low`, `high`, `open`, `close`, `volume`) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE `low` = VALUES(`low`), `high` = VALUES(`high`), `open` = VALUES(`open`), `close` = VALUES(`close`), `volume` = VALUES(`volume`);".format(
        coin, granularity)
      sql2 = "INSERT INTO `up_down_{}_{}`(`timestamp`, `upPercent`, `upPrice`, `downPercent`, `downPrice`) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE `upPercent` = VALUES(`upPercent`), `upPrice` = VALUES(`upPrice`), `downPercent` = VALUES(`downPercent`), `downPrice` = VALUES(`downPrice`);".format(
        coin, granularity)

      prices = []
      up_downs = []
      for row in rows:
        low = float_parser(row[1])
        high = float_parser(row[2])
        open = float_parser(row[3])
        close = float_parser(row[4])
        volume = float_parser(row[5])

        up_percent = (high - open) / open
        up_price = round(up_percent * open, 6)
        down_percent = (open - low) / open
        down_price = round(down_percent * open, 6)

        up_percent = round(up_percent * 100, 6)
        down_percent = round(down_percent * 100, 6)
        new_row = (
          row[0],
          datetime.fromtimestamp(row[0], tz=pytz.utc).isoformat()[:19],
          low,
          high,
          open,
          close,
          volume
        )
        prices.append(new_row)

        new_row = (
          row[0],
          up_percent,
          up_price,
          down_percent,
          down_price
        )
        up_downs.append(new_row)

      cursor.executemany(sql1, prices)
      cursor.executemany(sql2, up_downs)
      conn.commit()
  except ZeroDivisionError:
    print(row, low, high, open, close, volume, rows)
    conn.close()
    return {
      "statusCode": 500,
      "step": "Pull Data",
      "coin": coin,
      "granularity": granularity,
      "startTime": start_time
    }
  except Exception as inst:
    print(type(inst), inst.args, inst)
    # print(conn.cursor()._last_executed)
    conn.close()
    return {
      "statusCode": 500,
      "step": "Pull Data",
      "coin": coin,
      "granularity": granularity,
      "startTime": start_time
    }

  conn.close()

  return {
    "statusCode": 200,
    "step": "Pull Data",
    "body": "OK",
    "coin": coin,
    "granularity": granularity,
    "startTime": start_time
  }


s = sched.scheduler(time.time, time.sleep)
coins = start_timestamps.keys()
intervals = [granularities.day]


def create_tables():
  for coin in coins:
    for interval in intervals:
      print(create_table(coin, interval))


def interval_proc(sc):
  print("Doing sync...")
  try:
    for coin in coins:
      for interval in intervals:
        start_time = get_last_timestamp(coin, interval)
        print(pull_data(coin, interval, start_time))
        time.sleep(3)

  except:
    time.sleep(5 * 60)
  # do your stuff
  s.enter(60, 1, interval_proc, (sc,))


if __name__ == "__main__":
  create_tables()

  s.enter(0, 1, interval_proc, (s,))
  s.run()
