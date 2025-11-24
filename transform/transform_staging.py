import mysql.connector
import json
import re

# ------------------ Load config.json ------------------
with open("config/config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

staging_config = cfg["staging"]

# ------------------ Hàm chuẩn hóa ------------------
def parse_price(price_str):
    if not price_str:
        return 0.0
    price_str = price_str.lower().replace(',', '.').strip()
    try:
        if "triệu" in price_str:
            number = float(re.findall(r"\d+\.?\d*", price_str)[0])
            return ngit merge --abortumber * 1_000_000
        elif "tỷ" in price_str:
            number = float(re.findall(r"\d+\.?\d*", price_str)[0])
            return number * 1_000_000_000
        else:
            cleaned = "".join(c for c in price_str if c.isdigit() or c == ".")
            return float(cleaned)
    except:
        return 0.0

def parse_area(area_str):
    if not area_str:
        return 0.0
    area_str = area_str.lower().replace(",", ".").strip()
    try:
        return float(re.findall(r"\d+\.?\d*", area_str)[0])
    except:
        return 0.0

def parse_int_from_str(value_str):
    if not value_str:
        return 0
    match = re.search(r"\d+", value_str)
    return int(match.group()) if match else 0


# ------------------ RUN TRANSFORM ------------------
conn = mysql.connector.connect(**staging_config)
cursor = conn.cursor(dictionary=True)

# Lấy dữ liệu gốc từ Property_Temp
cursor.execute("SELECT * FROM Property_Temp;")
temp_rows = cursor.fetchall()

print(f"Fetched {len(temp_rows)} rows from Property_Temp")

# Xóa bảng Property trước khi ghi dữ liệu mới
cursor.execute("DELETE FROM Property;")

# Chuẩn bị insert
insert_sql = """
INSERT INTO Property (
    `key`, url, create_date, name, price, area, bedrooms, floors,
    description, street_width, property_type, street, ward, district,
    city, old_address, posting_date
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

count = 0

for row in temp_rows:
    cursor.execute(insert_sql, (
        row["key"],
        row["url"],
        row["create_date"],
        row["name"],
        parse_price(row["price"]),
        parse_area(row["area"]),
        parse_int_from_str(row["bedrooms"]),
        parse_int_from_str(row["floors"]),
        row["description"],
        row["street_width"],
        row["property_type"],
        row["street"],
        row["ward"],
        row["district"],
        row["city"],
        row["old_address"],
        row["posting_date"]
    ))
    count += 1

conn.commit()
cursor.close()
conn.close()

print(f"Transform completed → {count} rows inserted into Property")
