import json
import pandas as pd
import mysql.connector
from datetime import datetime
import os,sys
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)
from template.notification import send_error_email

#Chạy gửi mail báo lỗi tại local
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)


# ===== Load cấu hình từ config.json vào load_data_script=====

with open("config/config.json", "r", encoding="utf-8") as f:
    config_all = json.load(f)

staging_cfg = config_all["staging"]
control_cfg = config_all["control"]

# DB Staging
conn = mysql.connector.connect(**staging_cfg)
cursor = conn.cursor()

# DB Control
ctl_conn = mysql.connector.connect(**control_cfg)
ctl_cursor = ctl_conn.cursor()

# ============================
#  HÀM GHI LOG
# ============================
def normalize_path(path: str) -> str:
    #Chuẩn hóa đường dẫn thành dấu /
    return path.replace("\\", "/")

def start_process_log(process_name, file_id=None):
    """Tạo 1 dòng process_log trạng thái PS"""
    sql = """
        INSERT INTO process_log (file_id, process_name, status)
        VALUES (%s, %s, 'PS')
    """
    ctl_cursor.execute(sql, (file_id, process_name))
    ctl_conn.commit()
    return ctl_cursor.lastrowid


def update_process_success(process_id, file_id): 
    sql = """
        UPDATE process_log 
        SET status='SC', file_id=%s, updated_at=NOW() 
        WHERE process_id=%s
    """
    ctl_cursor.execute(sql, (file_id, process_id))
    ctl_conn.commit()


def update_process_fail(process_id, error_msg):
    sql = """
        UPDATE process_log 
        SET status='FL', updated_at=NOW(), error_msg=%s
        WHERE process_id=%s
    """
    ctl_cursor.execute(sql, (error_msg, process_id))
    ctl_conn.commit()


def create_file_log(file_path, row_count, status):
    sql = """
        INSERT INTO file_log (file_path, data_date, row_count, status)
        VALUES (%s, CURDATE(), %s, %s)
    """
    ctl_cursor.execute(sql, (normalize_path(file_path), row_count, status))
    ctl_conn.commit()
    return ctl_cursor.lastrowid


def update_file_log(file_id, status):
    sql = "UPDATE file_log SET status=%s, updated_at=NOW() WHERE file_id=%s"
    ctl_cursor.execute(sql, (status, file_id))
    ctl_conn.commit()

process_id = start_process_log("Load to Staging")

try:

    # ===== File Excel theo ngày =====
    today_str = datetime.now().strftime('%d_%m_%Y')
    file_name = f"bds_{today_str}.xlsx"
    file_path = os.path.join("data", file_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} không tồn tại!")

    print(f"Đang đọc file: {file_path}")
    # ===== Load Excel =====
    df = pd.read_excel(file_path, engine='openpyxl')
    df.columns = df.columns.str.strip()

    # ===== Detect cột Phòng ngủ và Diện tích =====
    bedroom_col = next((c for c in df.columns if "PN" in c or "Phòng ngủ" in c), "PN")
    area_col = next((c for c in df.columns if "DT" in c or "Diện tích" in c), "DT")

    # ===== Chuyển định dạng ngày =====
    def parse_date(val):
        if pd.isna(val):
            return None
        if isinstance(val, datetime):
            return val.strftime('%Y-%m-%d')
        try:
            return pd.to_datetime(val).strftime('%Y-%m-%d')
        except:
            return None

    if 'Ngày đăng' in df.columns:
        df['Ngày đăng'] = df['Ngày đăng'].apply(parse_date)



    # ===== Xóa dữ liệu cũ =====
    cursor.execute("TRUNCATE TABLE Property_Temp")
    print("Đã làm sạch bảng Property_Temp.")

    # ===== INSERT dữ liệu mới =====
    insert_query = """
    INSERT INTO Property_Temp 
    (`key`, url, create_date, name, price, area, old_address, street, ward, district, city, bedrooms, floors, street_width, description, posting_date, property_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Xử lý lỗi "nan": khi dữ liệu trống pandas trả về NaN (float), MySQL nhận được float NaN → chuyển thành chuỗi "nan" trong câu SQL gây lỗi
    def clean_text(val, default="N/A"):
        if pd.isna(val):
            return default
        val = str(val).strip()
        if val == "" or val.lower() == "nan":
            return default
        return val

    for idx, row in df.iterrows():
        cursor.execute(insert_query, (
            clean_text(row.get('Key')),
            clean_text(row.get('URL')),
            parse_date(row.get('Ngày cào')),
            clean_text(row.get('Tên')),
            clean_text(row.get('Giá')),
            clean_text(row.get(area_col)),
            clean_text(row.get('Địa chỉ')),
            clean_text(row.get('Đường')) if 'Đường' in df.columns else 'N/A',
            clean_text(row.get('Phường')),
            clean_text(row.get('Quận')),
            clean_text(row.get('Thành phố', 'Hồ Chí Minh')),
            clean_text(row.get(bedroom_col)),
            clean_text(row.get('Tầng')),
            clean_text(row.get('Lộ giới')),
            clean_text(row.get('Mô tả')),
            parse_date(row.get('Ngày đăng')),
            clean_text(row.get('Loại nhà', 'Khác'))
        ))

        
        print(f"Đã load row {idx + 1}/{len(df)}: {row.get('Tên', 'N/A')}")
    update_query = """
    UPDATE Property_Temp
    SET price = '7,9 tỷ/m²'
    WHERE `key` = '17672496'
    """
    cursor.execute(update_query)
    conn.commit()
    print("Đã cập nhật giá bản ghi có key = '17672496' thành 7,9 tỷ/m²")
    conn.commit()
    # ===== Ghi file_log =====
    file_id = create_file_log(file_path, len(df), "ST")   # ST = Staged

    # ===== Update process_log =====
    update_process_success(process_id, file_id)
    print(f"Đã load {len(df)} dòng vào bảng 'Property_Temp'.")
    #print(f"Đã load toàn bộ file {file_name} vào MySQL (overwrite toàn bộ)")
except Exception as e:
    error_msg = str(e)

    # Process log = fail
    update_process_fail(process_id, error_msg)

    # File_log = EF (Error File)
    file_id = create_file_log(file_path, 0, "EF")

    send_error_email("Load Staging Failed", error_msg)

    print("Lỗi:", error_msg)

finally:
    cursor.close()
    conn.close()
    ctl_cursor.close()
    ctl_conn.close()