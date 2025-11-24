# load_data_datawarehouse.py

import json
import mysql.connector
from datetime import datetime
import re
# script transforms and loads data from staging DB to data warehouse with SCD2

# ------------------ Load config.json ------------------
with open("config/config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

staging_config = cfg["staging"]
dw_config = cfg["datawarehouse"]

# ------------------ Compare old vs new for SCD2 ------------------
def has_changes(old, new):
    fields = [
        "url", "name", "price", "area", "bedrooms", "floors",
        "description", "street_width",
        "property_type_id", "location_id", "date_id"
    ]
    for f in fields:
        if old[f] != new[f]:
            return True
    return False

# ------------------ LOAD FROM STAGING ------------------
staging_conn = mysql.connector.connect(**staging_config)
cursor = staging_conn.cursor(dictionary=True)
cursor.execute("SELECT * FROM Property;")
staging_data = cursor.fetchall()
cursor.close()
staging_conn.close()

print(f"Fetched {len(staging_data)} rows from staging DB.")

# ------------------ CONNECT TO DW ------------------
dw_conn = mysql.connector.connect(**dw_config)
dw_cursor = dw_conn.cursor(dictionary=True)

# ------------------ ETL LOOP ------------------
for row in staging_data:

    key = row['key']
    url = row['url']
    create_date = row['create_date'] or datetime.today().date()
    name = row['name']
    price = row['price']
    area = row['area']
    bedrooms = row['bedrooms']
    floors = row['floors']
    description = row['description']
    street_width = row['street_width']

    # ------------------ DIM PropertyType ------------------
    property_type_name = row['property_type'] or "Unknown"

    dw_cursor.execute("SELECT property_type_id FROM PropertyType WHERE type_name=%s",
                      (property_type_name,))
    ptype = dw_cursor.fetchone()

    if ptype:
        property_type_id = ptype['property_type_id']
    else:
        dw_cursor.execute("INSERT INTO PropertyType (type_name) VALUES (%s)",
                          (property_type_name,))
        property_type_id = dw_cursor.lastrowid

    # ------------------ DIM Location ------------------
    street = row['street']
    ward = row['ward']
    district = row['district']
    city = row['city']
    old_address = row['old_address']

    dw_cursor.execute("""
        SELECT location_id FROM Location
        WHERE street=%s AND ward=%s AND district=%s AND city=%s AND old_address=%s
    """, (street, ward, district, city, old_address))
    loc = dw_cursor.fetchone()

    location_id = loc['location_id'] if loc else None

    if not location_id:
        dw_cursor.execute("""
            INSERT INTO Location (street, ward, district, city, old_address)
            VALUES (%s, %s, %s, %s, %s)
        """, (street, ward, district, city, old_address))
        location_id = dw_cursor.lastrowid

    # ------------------ DIM PostingDate ------------------
    posting_date = row['posting_date'] or datetime.today().date()

    dw_cursor.execute("SELECT date_id FROM PostingDate WHERE posting_date=%s",
                      (posting_date,))
    date = dw_cursor.fetchone()

    date_id = date['date_id'] if date else None

    if not date_id:
        dw_cursor.execute("INSERT INTO PostingDate (posting_date) VALUES (%s)",
                          (posting_date,))
        date_id = dw_cursor.lastrowid

    # ------------------ FACT PropertyListing (SCD2) ------------------
    dw_cursor.execute("""
        SELECT * FROM PropertyListing
        WHERE `key`=%s AND isCurrent=1
    """, (key,))
    old_record = dw_cursor.fetchone()

    new_record = {
        "url": url, "name": name, "price": price, "area": area,
        "bedrooms": bedrooms, "floors": floors, "description": description,
        "street_width": street_width, "property_type_id": property_type_id,
        "location_id": location_id, "date_id": date_id
    }

    # ---------- B1: Nếu không có record cũ → insert mới (TH tin lần đầu xuất hiện) ----------
    if not old_record:
        dw_cursor.execute("""
            INSERT INTO PropertyListing (
                `key`, url, create_date, name, price, area, bedrooms, floors,
                description, street_width, property_type_id, location_id,
                date_id, startDay, isCurrent
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURDATE(),1)
        """, (key, url, create_date, name, price, area, bedrooms, floors,
              description, street_width, property_type_id, location_id, date_id))
        continue

    # ---------- B2: Nếu có record cũ nhưng dữ liệu KHÔNG đổi → bỏ qua ----------
    if not has_changes(old_record, new_record):
        print(f"SKIP: No change for key = {key}")
        continue

    # ---------- B3: Nếu dữ liệu thay đổi → đóng bản cũ + tạo bản mới ----------
    print(f"UPDATE: Changes detected → key = {key}")

    dw_cursor.execute("""
        UPDATE PropertyListing
        SET endDay = CURDATE(), isCurrent = 0
        WHERE sk = %s
    """, (old_record['sk'],))

    dw_cursor.execute("""
        INSERT INTO PropertyListing (
            `key`, url, create_date, name, price, area, bedrooms, floors,
            description, street_width, property_type_id, location_id,
            date_id, startDay, isCurrent
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURDATE(),1)
    """, (key, url, create_date, name, price, area, bedrooms, floors,
          description, street_width, property_type_id, location_id, date_id))

# ------------------ Commit ------------------
dw_conn.commit()
dw_cursor.close()
dw_conn.close()

print("DW Load thành công — SCD2 cho FACT đã hoạt động đúng!")
