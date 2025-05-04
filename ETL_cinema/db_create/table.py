import subprocess
import os
import sys

# --- Cấu hình cho SQL Server Local ---
# !!! QUAN TRỌNG: Hãy sửa các giá trị này cho đúng với máy LOCAL của bạn !!!

# Tên Server SQL Server Local
sql_server = 'BLUE'  # <-- Tên server local của bạn

# Thông tin xác thực
use_windows_auth = True             # True: Dùng Windows Auth. False: Dùng SQL Server Auth.
# sql_username = 'your_sql_username'
# sql_password = 'your_sql_password'

# Đường dẫn đến file .sql của bạn
# QUAN TRỌNG: Đảm bảo file này đã được LƯU VỚI ENCODING UTF-8 (hoặc UTF-8 with BOM)
sql_file_path = r'D:\ETL\ETL_cinema\db_create\db.sql' # Sử dụng raw string (r'') hoặc \\ cho đường dẫn Windows

# Kiểm tra file tồn tại trước khi chạy
if not os.path.exists(sql_file_path):
    print(f"LỖI: Không tìm thấy file SQL tại đường dẫn: {sql_file_path}")
    print("Hãy đảm bảo đường dẫn chính xác và file đã được tạo.")
    exit()

# Đường dẫn đến sqlcmd
sqlcmd_path = 'sqlcmd'

# --- Xây dựng lệnh sqlcmd ---
command = [sqlcmd_path]
command.extend(['-S', sql_server]) # Tham số server local

if use_windows_auth:
    command.append('-E') # Xác thực Windows
    print(f"Sử dụng Windows Authentication để kết nối tới server local '{sql_server}'...")
else:
    # Cần bỏ comment và điền user/pass nếu dùng SQL Auth
    # if not sql_username or not sql_password:
    #     print("Lỗi: Cần cung cấp Username và Password cho SQL Server Authentication.")
    #     exit()
    # command.extend(['-U', sql_username])
    # command.extend(['-P', sql_password])
    # print(f"Sử dụng SQL Server Authentication (User: {sql_username})...")
    pass # Bỏ qua nếu đang dùng Windows Auth

# Chỉ định file input và yêu cầu thoát khi lỗi
command.extend(['-i', sql_file_path])
command.append('-b')

print(f"\nChuẩn bị thực thi lệnh sqlcmd:")
print(" ".join(f'"{c}"' if " " in c else c for c in command))
print(f"\nĐang chạy file '{os.path.basename(sql_file_path)}' trên SQL Server local (Không có try-catch)...")
print(f"(Đảm bảo file '{sql_file_path}' đã được lưu với encoding UTF-8)")

# --- Thực thi lệnh sqlcmd (Không có try-catch) ---
# Xác định encoding cho output console (để Python hiển thị đúng nếu sqlcmd xuất tiếng Việt)
console_encoding = sys.stdout.encoding if sys.stdout.encoding else 'utf-8'
print(f"Sử dụng encoding console: {console_encoding}")

# check=True sẽ dừng script Python nếu sqlcmd trả về lỗi
result = subprocess.run(command,
                        capture_output=True,
                        text=True,        # Đọc output dưới dạng text
                        check=True,       # Raise lỗi nếu sqlcmd thất bại
                        encoding=console_encoding, # Dùng encoding console để decode output
                        errors='replace') # Thay thế ký tự không thể decode trong output

# Chỉ chạy đến đây nếu sqlcmd trả về mã thành công (0)
print("\n--- Output từ sqlcmd (stdout) ---")
print(result.stdout if result.stdout else "(Không có output)")
print("---------------------------------")

# Kiểm tra xem có lỗi nào trong stderr không (đôi khi lỗi không làm thay đổi exit code nhưng vẫn có cảnh báo)
if result.stderr:
    print("\n--- Output lỗi/cảnh báo từ sqlcmd (stderr) ---")
    print(result.stderr)
    print("---------------------------------------------")

print("\n*** sqlcmd thực thi thành công file SQL trên server local (dựa trên mã trả về 0). ***")
print("(Lưu ý: Thành công ở đây nghĩa là sqlcmd chạy xong không báo lỗi nghiêm trọng. Hãy kiểm tra output để chắc chắn các lệnh SQL đã thực hiện đúng ý.)")

print("\n--- Script Python kết thúc ---")