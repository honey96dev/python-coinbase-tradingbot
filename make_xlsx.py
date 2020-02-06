import xlsxwriter
import pymysql.cursors
from datetime import datetime
from pathlib import Path

import mysql_config
from start_timestamps import start_timestamps
import granularities
from consts import months_json


def create_excel(coin, granularity, rows):
  Path("./output/{}".format(coin)).mkdir(parents=True, exist_ok=True)

  prev_year = ''
  prev_month = ''
  workbook = None
  sheet = None
  datetime_format = None
  r_index = 0
  for row in rows:
    timestamp = row['datetime']
    date = row['datetime'][:10]
    open = row['open']
    low = row['low']
    high = row['high']
    up_percent = row['upPercent']
    up_price = row['upPrice']
    down_percent = row['downPercent']
    down_price = row['downPrice']

    year = timestamp[:4]
    month = timestamp[5:7]
    day = timestamp[8:10]

    if prev_year != year:
      if workbook is not None:
        workbook.close()
      filename = "./output/{}/{}_{}.xlsx".format(coin, granularity, year)
      workbook = xlsxwriter.Workbook(filename)
      header_format = workbook.add_format({"bold": True, "align": "center"})
      datetime_format = workbook.add_format({"num_format": "d mmmm yyyy"})
      number_format = workbook.add_format({'num_format': '#,##0.00'})
    if prev_month != month:
      sheetname = months_json[month]
      sheet = workbook.add_worksheet(sheetname)
      
      sheet.set_column("A:A", 20)
      sheet.set_column("B:H", 10)

      sheet.write("A1", "Date", header_format)
      sheet.write("B1", "Down %", header_format)
      sheet.write("C1", "Down $", header_format)
      sheet.write("D1", "Low", header_format)
      sheet.write("E1", "Open", header_format)
      sheet.write("F1", "High", header_format)
      sheet.write("G1", "Up %", header_format)
      sheet.write("H1", "Up $", header_format)
      
      r_index = 2

    prev_year = year
    prev_month = month
    
    sheet.write_datetime("A{}".format(r_index), datetime.fromisoformat(timestamp), datetime_format)
    sheet.write_number("B{}".format(r_index), up_percent, number_format)
    sheet.write_number("C{}".format(r_index), up_price, number_format)
    sheet.write_number("D{}".format(r_index), low, number_format)
    sheet.write_number("E{}".format(r_index), open, number_format)
    sheet.write_number("F{}".format(r_index), high, number_format)
    sheet.write_number("G{}".format(r_index), down_percent, number_format)
    sheet.write_number("H{}".format(r_index), down_price, number_format)
    r_index += 1

  workbook.close()
  return


def create_file(coin, granularity):
  conn = pymysql.connect(host=mysql_config.host,
                         user=mysql_config.user,
                         password=mysql_config.password,
                         db=mysql_config.db,
                         charset=mysql_config.charset,
                         cursorclass=pymysql.cursors.DictCursor)
  try:
    with conn.cursor() as cursor:
      sql = "SELECT * FROM `up_down_view_{}_{}` ORDER BY `timestamp`;".format(coin, granularity)
      cursor.execute(sql)
      rows = cursor.fetchall()
      create_excel(coin, "day" if granularity == granularities.day else "", rows)

  except Exception as inst:
    print(type(inst), inst.args, inst)
    conn.close()
    return {
      "statusCode": 500,
      "step": "Create File",
      "coin": coin,
      "granularity": granularity
    }

  conn.close()

  return {
    "statusCode": 200,
    "step": "Create File",
    "coin": coin,
    "granularity": granularity
  }


if __name__ == "__main__":
  coins = start_timestamps.keys()
  intervals = [granularities.day]
  for coin in coins:
    for interval in intervals:
      create_file(coin, interval)