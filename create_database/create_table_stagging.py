import json
import mysql.connector

# Load config
with open("config/config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

staging_config = cfg["staging"]

conn = mysql.connector.connect(**staging_config)
cursor = conn.cursor()

# DROP TABLE
cursor.execute("DROP TABLE IF EXISTS Property;")
cursor.execute("DROP TABLE IF EXISTS Property_Temp;")
conn.commit()

# CREATE TABLE TEMP
cursor.execute("""
CREATE TABLE IF NOT EXISTS Property_Temp (
    temp_id INT AUTO_INCREMENT PRIMARY KEY,
    `key` TEXT,
    url TEXT,
    name TEXT,
    price TEXT,
    area TEXT,
    bedrooms TEXT,
    floors TEXT,
    street_width TEXT,
    description TEXT,
    street TEXT,
    ward TEXT,
    district TEXT,
    city TEXT,
    old_address TEXT,
    property_type TEXT,
    posting_date TEXT,
    create_date TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

# CREATE TABLE CLEAN
cursor.execute("""
CREATE TABLE IF NOT EXISTS Property (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `key` VARCHAR(50) UNIQUE,
    url TEXT,
    name VARCHAR(255),
    price DOUBLE,
    area DOUBLE,
    bedrooms INT,
    floors INT,
    street_width VARCHAR(50),
    description TEXT,
    street VARCHAR(255),
    ward VARCHAR(255),
    district VARCHAR(255),
    city VARCHAR(255),
    old_address TEXT,
    property_type VARCHAR(100),
    posting_date DATE,
    create_date DATE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""")

conn.commit()
cursor.close()
conn.close()

print("Đã tạo lại 2 bảng thành công!")
