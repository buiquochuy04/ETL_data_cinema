

# sqlserver_pipeline/assets.py
import pandas as pd
from dagster import asset, AssetIn, MetadataValue, Output, Out, multi_asset,AssetOut
from sqlalchemy import text, inspect
import subprocess
import sys
import os
from dagster import get_dagster_logger
from transform_data.transform import *
from sqlalchemy.dialects.mssql import NVARCHAR, INTEGER, DECIMAL,DATE, TIME, DATETIME


# Assuming resources.py is in the same directory or package
from .resources import SqlServerResource 

import subprocess
import sys
import os # Thêm import os
from dagster import asset, get_dagster_logger

CINEMA_CSV_PATH = 'D:\ETL\data\Cinema.csv'
ROOM_CSV_PATH = 'D:\ETL\data\Room.csv'
SEAT_CSV_PATH = 'D:\ETL\data\Seat.csv'
MOVIE_CSV_PATH = 'D:\ETL\data\movies.csv'
SHOWTIME_CSV_PATH = 'D:\ETL\data\Showtime.csv'
CUSTOMER_CSV_PATH = 'D:\ETL\data\Customer.csv'
EMPLOYEE_CSV_PATH = 'D:\ETL\data\Employee.csv'
DISCOUNT_CSV_PATH = 'D:\ETL\data\Discount.csv'
PRODUCT_CSV_PATH = 'D:\ETL\data\Product.csv'
INVOICE_CSV_PATH = 'D:\ETL\data\Invoice.csv'
TICKET_CSV_PATH = 'D:\ETL\data\Ticket.csv'
PRODUCTUSAGE_CSV_PATH = 'D:\ETL\data\Productusage.csv'

@asset(group_name='start')
def starting():  
  # Lấy đường dẫn đến trình thông dịch Python đang chạy
  python_executable = sys.executable 
  
  # Đường dẫn đến script của bạn (sử dụng raw string)
  script_path = r'D:\ETL\datascripts\fakedata.py' 
  
  # Tạo một bản sao của biến môi trường hiện tại
  # và đặt PYTHONIOENCODING thành utf-8 cho tiến trình con
  child_env = os.environ.copy()
  child_env['PYTHONIOENCODING'] = 'utf-8'
  
  # Sử dụng list cho các đối số của lệnh
  # Thêm `env=child_env` để truyền biến môi trường đã sửa đổi
  subprocess.run(
      [python_executable, script_path], 
      capture_output=True,  # Thu output (stdout, stderr)
      text=True,            # Giải mã output thành text (dùng UTF-8 nhờ env)
      check=True,           # Ném lỗi nếu script trả về mã lỗi khác 0
      encoding='utf-8',     # Chỉ định encoding để giải mã output (vẫn cần thiết)
      env=child_env         # Quan trọng: Truyền môi trường đã chỉnh sửa
  )


# --- Data Generation / Crawling ---
@asset(
  # Định nghĩa các output assets một cách tường minh bằng AssetOut
  # Sử dụng 'outs' (số nhiều) với @multi_asset
  # Vẫn có thể thêm ins và group_name như bình thường
  ins={"started": AssetIn(key="starting")},
  group_name="crawl"
  # compute_kind="python" # Tùy chọn: chỉ định loại tính toán
)
def crawl_data(started):
    return {
        "cinema_raw": pd.read_csv(CINEMA_CSV_PATH),
        "room_raw": pd.read_csv(ROOM_CSV_PATH),
        "seat_raw": pd.read_csv(SEAT_CSV_PATH),
        "movie_raw": pd.read_csv(MOVIE_CSV_PATH),
        "showtime_raw": pd.read_csv(SHOWTIME_CSV_PATH),
        "customer_raw": pd.read_csv(CUSTOMER_CSV_PATH),
        "employee_raw": pd.read_csv(EMPLOYEE_CSV_PATH),
        "discount_raw": pd.read_csv(DISCOUNT_CSV_PATH),
        "product_raw": pd.read_csv(PRODUCT_CSV_PATH),
        "invoice_raw": pd.read_csv(INVOICE_CSV_PATH),
        "ticket_raw": pd.read_csv(TICKET_CSV_PATH),
        "productusage_raw": pd.read_csv(PRODUCTUSAGE_CSV_PATH),
    }

@asset(
  # Định nghĩa các output assets một cách tường minh bằng AssetOut
  # Sử dụng 'outs' (số nhiều) với @multi_asset
  # Vẫn có thể thêm ins và group_name như bình thường
  ins={"raw_data": AssetIn(key="crawl_data")},
  group_name="tranformed"
  # compute_kind="python" # Tùy chọn: chỉ định loại tính toán
)
def transform_data(raw_data: dict) -> dict:
    cinema_data = raw_data["cinema_raw"]
    room_data = raw_data["room_raw"]
    seat_data = raw_data["seat_raw"]
    movie_data = raw_data["movie_raw"]
    showtime_data = raw_data["showtime_raw"]
    customer_data = raw_data["customer_raw"]
    employee_data = raw_data["employee_raw"]
    discount_data = raw_data["discount_raw"]
    product_data = raw_data["product_raw"]
    invoice_data = raw_data["invoice_raw"]
    ticket_data = raw_data["ticket_raw"]
    productusage_data = raw_data["productusage_raw"]
    return {
        "cinema_raw": transformed_cinema(cinema_data),
        "room_raw": transformed_room(room_data),
        "seat_raw": transformed_seat(seat_data),
        "movie_raw": transformed_movie(movie_data),
        "showtime_raw": transformed_showtime(showtime_data),
        "customer_raw": transformed_customer(customer_data),
        "employee_raw": transformed_employee(employee_data),
        "discount_raw": transformed_discount(discount_data),
        "product_raw": transformed_product(product_data),
        "invoice_raw": transformed_invoice(invoice_data),
        "ticket_raw": transformed_ticket(ticket_data),
        "productusage_raw": transformed_productusage(productusage_data)
    }

# # # Note: group_name matches the blue box in the visualization
# @asset(
#     ins={"raw_data": AssetIn(key="crawl_data")},
#     group_name="transform_data"
# )
# def transform_cinema(raw_data: dict) -> pd.DataFrame:
#     cinema_data = raw_data["cinema_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_cinema(cinema_data)  # Placeholder
#     return {
#         **raw_data,
#         'cinema_raw': transformed_df
#     }

# @asset(
#     ins={
#         "raw_data": AssetIn(key="transform_cinema")
#     },
#     group_name="transform_data"
# )
# def transform_room(raw_data: dict) -> pd.DataFrame:
#     room_data = raw_data["room_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_room(room_data)  # Placeholder
#     return transformed_df

# @asset(
#     ins={
#         "raw_data": AssetIn(key="transform_cinema")
#     },
#     group_name="transform_data"
# )
# def transform_seat(raw_data: dict) -> pd.DataFrame:
#     seat_data = raw_data["seat_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_seat(seat_data)  # Placeholder
#     return transformed_df

# @asset(
#     ins={"raw_data": AssetIn(key="crawl_data")},
#     group_name="transform_data"
# )
# def transform_movie(raw_data: dict) -> pd.DataFrame:
#     movie_data = raw_data["movie_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_movie(movie_data)  # Placeholder
#     return {
#         **raw_data,
#         'movie_raw': transformed_df
#     }

# @asset(
#     ins={
#         "room_data": AssetIn(key='transform_room'),
#         "movie_data": AssetIn(key='transform_movie')
#     },
#     group_name="transform_data"
# )
# def transform_showtime(movie_data: dict, room_data: pd.DataFrame) -> pd.DataFrame:
#     showtime_data = movie_data["movie_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_showtime(showtime_data)  # Placeholder
#     return {
#         **movie_data,
#         'showtime_raw': transformed_df
#     }

# @asset(
#     ins={"raw_data": AssetIn(key="crawl_data")},
#     group_name="transform_data"
# )
# def transform_customer(raw_data: dict) -> pd.DataFrame:
#     customer_data = raw_data["customer_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_customer(customer_data)  # Placeholder
#     return {
#         **raw_data,
#         'customer_raw': transformed_df
#     }

# @asset(
#     ins={"raw_data": AssetIn(key="crawl_data")},
#     group_name="transform_data"
# )
# def transform_employee(raw_data: dict) -> pd.DataFrame:
#     employee_data = raw_data["employee_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_movie(employee_data)  # Placeholder
#     return {
#         **raw_data,
#         'employee_raw': transformed_df
#     }

# @asset(
#     ins={"raw_data": AssetIn(key="crawl_data")},
#     group_name="transform_data"
# )
# def transform_discount(raw_data: dict) -> pd.DataFrame:
#     discount_data = raw_data["discount_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_discount(discount_data)  # Placeholder
#     return {
#         **raw_data,
#         'discount_raw': transformed_df
#     }

# @asset(
#     ins={"raw_data": AssetIn(key="crawl_data")},
#     group_name="transform_data"
# )
# def transform_product(raw_data: dict) -> pd.DataFrame:
#     product_data = raw_data["product_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_product(product_data)  # Placeholder
#     return {
#         **raw_data,
#         'product_raw': transformed_df
#     }

# @asset(
#     ins={
#         "customer_data": AssetIn(key='transform_customer'),
#         "employee_data": AssetIn(key='transform_employee'),
#         "discount_data": AssetIn(key='transform_discount'),
#     },
#     group_name="transform_data"
# )
# def transform_invoice(customer_data: dict, employee_data: dict, discount_data: dict) -> pd.DataFrame:
#     invoice_data = discount_data["invoice_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_invoice(invoice_data)  # Placeholder
#     return {
#         **discount_data,
#         'invoice_raw': transformed_df
#     }

# @asset(
#     ins={
#         "invoice_data": AssetIn(key='transform_invoice'),
#         "showtime_data": AssetIn(key='transform_showtime'),
#         "seat_data": AssetIn(key='transform_seat'),
#     },
#     group_name="transform_data"
# )
# def transform_ticket(invoice_data: dict, showtime_data: dict, seat_data: dict) -> pd.DataFrame:
#     ticket_data = invoice_data["ticket_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_ticket(ticket_data)  # Placeholder
#     return {
#         **invoice_data,
#         'ticket_raw': transformed_df
#     }

# @asset(
#     ins={
#         "invoice_data": AssetIn(key='transform_invoice'),
#         "product_data": AssetIn(key='transform_product')
#     },
#     group_name="transform_data"
# )
# def transform_productusage(invoice_data: dict, product_data: dict) -> pd.DataFrame:
#     productusage_data = invoice_data["productusage_raw"]
#     # --- Thêm logic transform thực tế của bạn vào đây ---
#     transformed_df = transformed_productusage(productusage_data)  # Placeholder
#     return {
#         **invoice_data,
#         'productusage_raw': transformed_df
#     }
# --- SQL Server Processing Assets ---
# Note: group_name matches the blue box for SQL processing

@asset(
    ins={"transformed_data": AssetIn(key="transform_data")},
    group_name="sqlserver_processing",
    compute_kind="sqlserver" # Optional: helps categorize in UI
)
def set_up_database(context, transformed_data: dict, sql_server: SqlServerResource) -> dict:
    subprocess.run([sys.executable, r"D:\ETL\ETL_cinema\db_create\table.py"])
    return{ 
       **transformed_data
    }


@asset(
    ins={"db": AssetIn(key="set_up_database")},
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_cinema(context, db: dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "cinema_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("cinema", con=connection, if_exists='append', index=False, dtype={"status": NVARCHAR(255), "name": NVARCHAR(255), "address": NVARCHAR(255), "note": NVARCHAR(500)})
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="load_data_cinema")},
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_room(context, db: dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "room_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("room", con=connection, if_exists='append', index=False, dtype={"status": NVARCHAR(255), "name": NVARCHAR(255), "type": NVARCHAR(255), "seatCount": INTEGER, "cinema_id": INTEGER})
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="load_data_cinema")},
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_seat(context, db: dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "seat_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("seat", con=connection, if_exists='append', index=False, dtype={"status": NVARCHAR(255), "position": NVARCHAR(255), "type": NVARCHAR(255), "price": DECIMAL(10,2), "room_id": INTEGER})
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="set_up_database")},
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_movie(context, db: dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "movie_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("movie", con=connection, if_exists='append', index=False, dtype={"title": NVARCHAR(255),
                       "genre": NVARCHAR(100),
                       "duration": INTEGER,
                       "releaseDate": DATE,
                       "poster": NVARCHAR(500),
                       "trailer": NVARCHAR(500),
                       "description": NVARCHAR(3999),
                       "ageRating": NVARCHAR(10),
                       "status": NVARCHAR(50),
                       "director": NVARCHAR(100),
                       "mainActor": NVARCHAR(255),
                       "language": NVARCHAR(50)})
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="load_data_movie"),
         "room": AssetIn(key="load_data_room")
        },
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_showtime(context, db: dict, room:dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "showtime_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("showtime", con=connection, if_exists='append', index=False, dtype={
                "startTime": TIME,
                "endTime": TIME,
                "showDate": DATE,
                "room_id": INTEGER,
                "movie_id": INTEGER,
            })
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="set_up_database")},
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_customer(context, db: dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "customer_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("customer", con=connection, if_exists='append', index=False, dtype={
                "fullName": NVARCHAR(100),
                "phoneNumber": NVARCHAR(20),
                "username": NVARCHAR(50),
                "password": NVARCHAR(255),
                "email": NVARCHAR(100)
            })
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="set_up_database")},
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_employee(context, db: dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "employee_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("employee", con=connection, if_exists='append', index=False, dtype={
                "fullName": NVARCHAR(100),
                "phoneNumber": NVARCHAR(20),
                "position": NVARCHAR(50),
                "username": NVARCHAR(50),
                "password": NVARCHAR(255),
                "email": NVARCHAR(100)
            })
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="set_up_database")},
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_discount(context, db: dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "discount_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("discount", con=connection, if_exists='append', index=False, dtype={
                "name": NVARCHAR(100),
                "type": NVARCHAR(50),
                "description": NVARCHAR(500),
                "quantity": INTEGER,
                "discountValue": DECIMAL(10,2),
                "startDate": DATE,
                "endDate": DATE
            })
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="set_up_database")},
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_product(context, db: dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "product_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("product", con=connection, if_exists='append', index=False, dtype={
                "name": NVARCHAR(100),
                "price": DECIMAL(10,2),
                "unit": NVARCHAR(20),
                "quantity": INTEGER,
                "description": NVARCHAR(500)
            })
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="load_data_customer"),
        "discount": AssetIn(key="load_data_discount"),
        "employee": AssetIn(key="load_data_employee")
        },
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_invoice(context, db: dict, discount:dict, employee:dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "invoice_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("invoice", con=connection, if_exists='append', index=False, dtype={
                "createDate": DATETIME,
                "totalDiscount": DECIMAL(10,2),
                "totalAmount": DECIMAL(10,2),
                "paymentMethod": NVARCHAR(50),
                "qrCode": NVARCHAR(255),
                "status": NVARCHAR(100),
                "note": NVARCHAR(500),
                "customer_id": INTEGER,
                "discount_id": INTEGER,
                "employee_id": INTEGER
            })
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="load_data_seat"),
        "showtime": AssetIn(key="load_data_showtime"),
        "seat": AssetIn(key="load_data_invoice")
        },
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_ticket(context, db: dict, showtime:dict, seat:dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "ticket_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("ticket", con=connection, if_exists='append', index=False, dtype={
                "bookingDate": DATETIME,
                "price": DECIMAL(10,2),
                "qrCode": NVARCHAR(255),
                "showtime_id": INTEGER,
                "seat_id": INTEGER,
                "invoice_id": INTEGER
            })
    return{
        **db
    }

@asset(
    ins={"db": AssetIn(key="load_data_invoice"),
        "product": AssetIn(key="load_data_product")
        },
    group_name="sqlserver_processing",
    compute_kind="sqlserver"
)
def load_data_productusage(context, db: dict, product:dict, sql_server: SqlServerResource) -> dict:
    """Loads fact data into the SQL Server fact table."""
    engine = sql_server.engine
    table_name = "productusage_raw"
    df = db[table_name]

    with engine.connect() as connection:
        with connection.begin(): # Use transaction
            df.to_sql("productusage", con=connection, if_exists='append', index=False, dtype={
                "quantity": INTEGER,
                "purchasePrice": DECIMAL(10,2),
                "product_id": INTEGER,
                "invoice_id": INTEGER
            })
    return{
        **db
    }