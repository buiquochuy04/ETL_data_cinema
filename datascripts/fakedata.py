import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta, date, time
import uuid # Dùng cho mã QR
import os # Dùng để tạo thư mục output


# --- Cấu hình ---
NUM_CINEMAS = 3
ROOMS_PER_CINEMA_MIN = 3
ROOMS_PER_CINEMA_MAX = 6
SEATS_PER_ROOM_MIN_ROWS = 5
SEATS_PER_ROOM_MAX_ROWS = 10
SEATS_PER_ROOM_MIN_COLS = 8
SEATS_PER_ROOM_MAX_COLS = 15

NUM_CUSTOMERS = 100
NUM_EMPLOYEES = 20
NUM_DISCOUNTS = 15
NUM_PRODUCTS = 10
NUM_SHOWTIMES_PER_MOVIE_AVG = 5 # Số lượng suất chiếu trung bình mỗi phim
NUM_INVOICES = 200

# Số ngày tối đa trước suất chiếu có thể đặt vé
BOOKING_WINDOW_DAYS = 7

# Đường dẫn file Movie CSV
MOVIE_CSV_PATH = r'D:\ETL\data\movies.csv'
OUTPUT_DIR = r'D:\ETL\data'

# --- Khởi tạo Faker ---
# Sử dụng locale tiếng Việt nếu muốn dữ liệu tên, địa chỉ tiếng Việt
fake = Faker('vi_VN')
# fake = Faker() # Giữ tiếng Anh cho dữ liệu fake đa dạng hơn hoặc nếu không cần TV

# --- Tạo thư mục output nếu chưa có ---
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"Đã tạo thư mục output: {OUTPUT_DIR}")

# --- Danh sách để lưu trữ ID và dữ liệu đã tạo ---
cinemas_data = []
rooms_data = []
seats_data = []
movies_data = []
customers_data = []
employees_data = []
discounts_data = []
products_data = []
showtimes_data = []
invoices_data = []
tickets_data = []
product_usages_data = []

# --- Hàm tiện ích ---
def random_status(options=["Hoạt động", "Không hoạt động", "Bảo trì"], weights=None):
    """Chọn ngẫu nhiên một trạng thái từ danh sách."""
    return random.choices(options, weights=weights, k=1)[0]

# Theo dõi các giá trị duy nhất
customer_usernames = set()
employee_usernames = set()
discount_names = set()
room_cinema_names = set() # Để đảm bảo tên phòng + cinema_id là duy nhất
seat_room_positions = set() # Để đảm bảo vị trí ghế + room_id là duy nhất

# --- Tạo dữ liệu ---

print("Đang tạo dữ liệu Rạp chiếu (Cinema)...")
for i in range(1, NUM_CINEMAS + 1):
    cinema = {
        'id': i,
        'name': fake.company() + " Cinema",
        'address': fake.address().replace('\n', ', '),
        'note': fake.text(max_nb_chars=200) if random.random() > 0.5 else None,
        'status': random_status(['Hoạt động', 'Đang bảo trì', 'Đóng cửa'], weights=[0.8, 0.1, 0.1])
    }
    cinemas_data.append(cinema)

print("Đang tạo dữ liệu Phòng chiếu (Room) và Ghế (Seat)...")
room_id_counter = 1
seat_id_counter = 1
for cinema in cinemas_data:
    num_rooms = random.randint(ROOMS_PER_CINEMA_MIN, ROOMS_PER_CINEMA_MAX)
    for j in range(num_rooms):
        room_name = f"Phòng {chr(65+j)}" # Phòng A, Phòng B...
        room_key = (cinema['id'], room_name)
        # Đảm bảo tên phòng là duy nhất trong rạp
        while room_key in room_cinema_names:
             room_name = f"Phòng {chr(65+j)}{random.randint(1,5)}" # Thêm số nếu trùng
             room_key = (cinema['id'], room_name)
        room_cinema_names.add(room_key)

        room_type = random.choice(["Tiêu chuẩn", "VIP", "IMAX", "3D"])
        num_rows = random.randint(SEATS_PER_ROOM_MIN_ROWS, SEATS_PER_ROOM_MAX_ROWS)
        num_cols = random.randint(SEATS_PER_ROOM_MIN_COLS, SEATS_PER_ROOM_MAX_COLS)
        seat_count = num_rows * num_cols

        room = {
            'id': room_id_counter,
            'name': room_name,
            'type': room_type,
            'seatCount': seat_count,
            'status': random_status(['Hoạt động', 'Đang bảo trì', 'Đóng cửa'], weights=[0.85, 0.1, 0.05]),
            'cinema_id': cinema['id']
        }
        rooms_data.append(room)

        # Tạo ghế cho phòng này
        for row in range(num_rows):
            row_char = chr(65 + row) # A, B, C...
            for col in range(1, num_cols + 1):
                seat_pos = f"{row_char}{col}"
                seat_key = (room['id'], seat_pos)
                # Không cần check seat_key vì logic tạo đảm bảo duy nhất trừ khi có lỗi lặp

                seat_type = "VIP" if row_char in ['A', 'B'] and room_type != "Tiêu chuẩn" else "Thường"
                seat_price = random.uniform(70000, 120000) if seat_type == "Thường" else random.uniform(150000, 250000)
                if room_type == "IMAX":
                     seat_price *= 1.2
                elif room_type == "3D":
                     seat_price *= 1.1
                elif room_type == "VIP":
                     seat_price *= 1.5 # Phòng VIP ghế thường cũng đắt hơn

                seat = {
                    'id': seat_id_counter,
                    'position': seat_pos,
                    'type': seat_type,
                    'status': random_status(['Trống', 'Đã đặt', 'Bảo trì'], weights=[0.9, 0.05, 0.05]),
                    'price': round(seat_price, -2), # Làm tròn đến hàng trăm VNĐ
                    'room_id': room['id']
                }
                seats_data.append(seat)
                seat_id_counter += 1
        room_id_counter += 1

print("Đang tải dữ liệu Phim (Movie) từ CSV...")
try:
    movie_df = pd.read_csv(MOVIE_CSV_PATH)
    # Chuyển releaseDate sang datetime objects, lỗi sẽ thành NaT
    movie_df['releaseDate'] = pd.to_datetime(movie_df['releaseDate'], errors='coerce')
    # Điền dữ liệu thiếu nếu cần (tùy chọn)
    # Bỏ các dòng không phân tích được releaseDate
    movie_df.dropna(subset=['releaseDate'], inplace=True)

    # Chuyển DataFrame thành list of dicts
    movies_data = movie_df.to_dict('records')

    # Đảm bảo ID phim là tuần tự hoặc sử dụng ID từ CSV nếu ổn
    # Giả sử cột 'id' trong CSV là khóa chính và dùng được
    # Cập nhật lại ID nếu cần để đảm bảo tính duy nhất và tuần tự nếu CSV gốc không đảm bảo
    # for idx, movie in enumerate(movies_data):
    #     movie['id'] = idx + 1 # Gán lại ID tuần tự

    print(f"Đã tải {len(movies_data)} phim.")
except FileNotFoundError:
    print(f"Lỗi: Không tìm thấy file {MOVIE_CSV_PATH}. Không thể tạo dữ liệu liên quan đến phim.")
    # Thoát hoặc tiếp tục mà không có dữ liệu phim? Hiện tại sẽ thoát.
    exit()
except Exception as e:
    print(f"Lỗi khi đọc hoặc xử lý {MOVIE_CSV_PATH}: {e}")
    exit()


print("Đang tạo dữ liệu Khách hàng (Customer)...")
for i in range(1, NUM_CUSTOMERS + 1):
    username = fake.user_name()
    while username in customer_usernames:
        username = fake.user_name() + str(random.randint(1,99))
    customer_usernames.add(username)

    customer = {
        'id': i,
        'fullName': fake.name(), # Sử dụng fake.name() của locale đã chọn
        'phoneNumber': fake.phone_number()[:20], # Giới hạn độ dài
        'username': username,
        'password': fake.password(), # Trong ứng dụng thật, cần hash mật khẩu!
        'email': fake.email()
    }
    customers_data.append(customer)

print("Đang tạo dữ liệu Nhân viên (Employee)...")
for i in range(1, NUM_EMPLOYEES + 1):
    username = fake.user_name() + "_nv"
    while username in employee_usernames:
        username = fake.user_name() + "_nv" + str(random.randint(1,99))
    employee_usernames.add(username)

    employee = {
        'id': i,
        'fullName': fake.name(),
        'phoneNumber': fake.phone_number()[:20],
        'position': random.choice(["Quản lý", "Thu ngân", "Soát vé", "Kỹ thuật"]),
        'username': username,
        'password': fake.password(), # Cần hash!
        'email': fake.email()
    }
    employees_data.append(employee)

print("Đang tạo dữ liệu Giảm giá (Discount)...")
for i in range(1, NUM_DISCOUNTS + 1):
    name = f"KM {random.choice(['Hè', 'Tết', 'Sinh viên', 'Thứ 3', 'Cuối tuần'])} {random.randint(10, 99)}"
    while name in discount_names:
        name = f"KM {random.choice(['Đặc biệt', 'Vui vẻ', 'Giảm sốc'])} {random.randint(100, 999)}"
    discount_names.add(name)

    discount_type = random.choice(["Phần trăm", "Số tiền cố định", "Đồng giá"])
    value = 0
    if discount_type == "Phần trăm":
        value = random.choice([10, 15, 20, 25, 30, 50])
    elif discount_type == "Số tiền cố định":
         value = random.choice([10000, 15000, 20000, 30000, 50000])
    else: # Đồng giá
        value = random.choice([49000, 69000, 99000])

    start_date = fake.date_between(start_date='-1y', end_date='+3m')
    # Đảm bảo endDate sau startDate ít nhất 1 tháng
    end_date = fake.date_between(start_date=start_date + timedelta(days=30), end_date='+1y')

    discount = {
        'id': i,
        'name': name,
        'type': discount_type,
        'description': fake.sentence(nb_words=15),
        'quantity': random.randint(50, 500), # Số lượng mã có thể sử dụng
        'discountValue': round(value),
        'startDate': start_date, # Giữ kiểu date object để xử lý
        'endDate': end_date     # Giữ kiểu date object
    }
    discounts_data.append(discount)

print("Đang tạo dữ liệu Sản phẩm (Product)...")
product_options = ["Bắp rang bơ", "Nước ngọt", "Kẹo", "Nachos", "Xúc xích", "Nước suối", "Kem", "Combo"]
units = {"Bắp rang bơ": "Hộp", "Nước ngọt": "Ly", "Kẹo": "Gói", "Nachos": "Khay", "Xúc xích": "Cây", "Nước suối": "Chai", "Kem": "Ly", "Combo": "Phần"}
base_prices = {"Bắp rang bơ": 55000, "Nước ngọt": 40000, "Kẹo": 25000, "Nachos": 60000, "Xúc xích": 45000, "Nước suối": 20000, "Kem": 40000, "Combo": 120000}

used_product_names = set()
for i in range(1, NUM_PRODUCTS + 1):
     base_name = random.choice(product_options)
     size = ""
     if base_name in ["Bắp rang bơ", "Nước ngọt"]:
          size = f" {random.choice(['Nhỏ', 'Vừa', 'Lớn'])}"

     full_name = base_name + size
     if full_name in used_product_names: # Tạo tên khác nếu trùng
         full_name = f"{base_name} {random.choice(['Đặc biệt', 'XXL', 'Mini'])}"
         if full_name in used_product_names: continue # Bỏ qua nếu vẫn trùng (hiếm)
     used_product_names.add(full_name)


     price = base_prices.get(base_name, 50000)
     if "Vừa" in full_name: price *= 1.2
     if "Lớn" in full_name or "XXL" in full_name: price *= 1.5
     if "Combo" in full_name: price = base_prices.get(base_name, 120000) + random.randint(-10000, 10000) # Biến thể giá combo
     if "Mini" in full_name: price *= 0.7

     product = {
         'id': i,
         'name': full_name,
         'price': round(price, -2), # Làm tròn VNĐ
         'unit': units.get(base_name, "Cái"),
         'quantity': random.randint(100, 1000), # Số lượng tồn kho
         'description': f"{full_name} thơm ngon"
     }
     products_data.append(product)

print("Đang tạo dữ liệu Suất chiếu (ShowTime)...")
showtime_id_counter = 1
# Theo dõi lịch chiếu để tránh trùng lặp quá nhiều (kiểm tra cơ bản)
showtime_schedule = {} # Key: (room_id, showDate), Value: list of (startTime, endTime) tuples

if movies_data and rooms_data:
    for movie in movies_data:
        # Đảm bảo ngày phát hành hợp lệ
        if pd.isna(movie['releaseDate']):
            print(f"Bỏ qua phim ID {movie.get('id', 'N/A')} do ngày phát hành không hợp lệ.")
            continue
        # release_date đã là datetime object từ pandas
        release_date = movie['releaseDate'].date() # Chỉ lấy phần ngày

        # Tạo một vài suất chiếu cho phim này
        num_showtimes_for_this_movie = random.randint(1, NUM_SHOWTIMES_PER_MOVIE_AVG * 2)

        for _ in range(num_showtimes_for_this_movie):
            # Chọn phòng ngẫu nhiên
            room = random.choice(rooms_data)
            room_id = room['id']

            # Tạo showDate *sau* hoặc *vào* ngày phát hành
            # Tạo suất chiếu trong vòng 90 ngày kể từ ngày phát hành
            try:
                 show_date = fake.date_between(start_date=release_date, end_date=release_date + timedelta(days=90))
            except ValueError: # Xử lý trường hợp ngày phát hành ở tương lai xa
                 show_date = fake.date_between(start_date=release_date, end_date='+90d')


            # Tạo startTime (ví dụ: từ 10:00 đến 22:00)
            start_hour = random.randint(10, 21)
            start_minute = random.choice([0, 15, 30, 45])
            # startTime là đối tượng time
            start_time = time(hour=start_hour, minute=start_minute)

            # Tính endTime
            time_duration = movie.get('duration', 120)
            duration_minutes = 120 if pd.isna(time_duration) else int(time_duration) # Mặc định nếu thiếu
            # Kết hợp ngày và giờ để tính toán dễ dàng
            start_datetime = datetime.combine(show_date, start_time)
            end_datetime = start_datetime + timedelta(minutes=duration_minutes)
            end_time = end_datetime.time() # Chỉ lấy phần thời gian

            # Kiểm tra trùng lặp cơ bản (optional nhưng nên có)
            schedule_key = (room_id, show_date)
            room_day_schedule = showtime_schedule.get(schedule_key, [])

            is_overlap = False
            current_start_dt = start_datetime
            current_end_dt = end_datetime

            for existing_start, existing_end in room_day_schedule:
                 # existing_start, existing_end cũng là datetime objects
                 # Kiểm tra trùng lặp: (StartA < EndB) and (EndA > StartB)
                 # Thêm khoảng nghỉ giữa các suất (ví dụ 15 phút)
                 buffer = timedelta(minutes=15)
                 if current_start_dt < (existing_end + buffer) and (current_end_dt + buffer) > existing_start:
                      is_overlap = True
                      break

            if not is_overlap:
                showtime = {
                    'id': showtime_id_counter,
                    # Giữ kiểu time/date object để pandas xử lý khi ghi CSV
                    'startTime': start_time,
                    'endTime': end_time,
                    'showDate': show_date,
                    'room_id': room_id,
                    'movie_id': movie['id'] # Giả sử cột 'id' tồn tại trong CSV
                }
                showtimes_data.append(showtime)
                # Thêm vào lịch để kiểm tra trùng lặp
                if schedule_key not in showtime_schedule:
                    showtime_schedule[schedule_key] = []
                # Lưu datetime objects để so sánh chính xác
                showtime_schedule[schedule_key].append((current_start_dt, current_end_dt))

                showtime_id_counter += 1
            # else: print(f"Bỏ qua suất chiếu do trùng lặp Phòng {room_id} ngày {show_date}")

else:
    print("Bỏ qua tạo Suất chiếu vì danh sách Phim hoặc Phòng trống.")


print("Đang tạo dữ liệu Hóa đơn (Invoice), Vé (Ticket), và Chi tiết sử dụng sản phẩm (ProductUsage)...")
invoice_id_counter = 1
ticket_id_counter = 1
product_usage_id_counter = 1
# Theo dõi ghế đã đặt cho từng suất chiếu để tránh đặt trùng
# Key: showtime_id, Value: set of seat_ids
booked_seats_per_showtime = {}

if customers_data and employees_data and showtimes_data and seats_data and products_data:
    # Lọc các suất chiếu trong tương lai gần hoặc quá khứ gần để tạo hóa đơn hợp lý
    relevant_showtimes = []
    today = datetime.now().date()
    for st in showtimes_data:
         if isinstance(st['showDate'], str): # Nếu showDate là string, parse nó
             try:
                st_date = datetime.strptime(st['showDate'], '%Y-%m-%d').date()
             except ValueError: continue # Bỏ qua nếu định dạng sai
         else: # Nếu là date object
             st_date = st['showDate']

         # Chỉ lấy suất chiếu trong khoảng 90 ngày trước và 30 ngày sau hôm nay
         if (today - timedelta(days=90)) <= st_date <= (today + timedelta(days=30)):
              relevant_showtimes.append(st)

    if not relevant_showtimes:
        print("Không tìm thấy suất chiếu phù hợp để tạo hóa đơn.")
    else:
        for i in range(min(NUM_INVOICES, len(relevant_showtimes) * 5)): # Giới hạn số hóa đơn hợp lý
            # 1. Chọn một Suất chiếu để đặt vé
            selected_showtime = random.choice(relevant_showtimes)
            showtime_id = selected_showtime['id']
            room_id = selected_showtime['room_id']

            # Kết hợp ngày và giờ chiếu
            show_date = selected_showtime['showDate']
            start_time = selected_showtime['startTime']
            if isinstance(show_date, str): show_date = datetime.strptime(show_date, '%Y-%m-%d').date()
            if isinstance(start_time, str): start_time = datetime.strptime(start_time, '%H:%M:%S').time()

            show_datetime = datetime.combine(show_date, start_time)


            # 2. Xác định Ngày đặt vé (createDate cho Hóa đơn)
            # Phải trước giờ chiếu, trong khoảng BOOKING_WINDOW_DAYS
            latest_booking_time = show_datetime - timedelta(minutes=15) # Đặt trước ít nhất 15 phút
            earliest_booking_time = show_datetime - timedelta(days=BOOKING_WINDOW_DAYS)

            # Đảm bảo earliest không sau latest
            if earliest_booking_time > latest_booking_time:
                 earliest_booking_time = latest_booking_time - timedelta(hours=1)
                 if earliest_booking_time > latest_booking_time:
                      earliest_booking_time = latest_booking_time - timedelta(minutes=5)


            # Tạo ngày giờ ngẫu nhiên trong khoảng hợp lệ
            try:
                # Faker có thể cần start_date < end_date
                if earliest_booking_time < latest_booking_time:
                     create_date = fake.date_time_between(start_date=earliest_booking_time, end_date=latest_booking_time)
                else: # Nếu thời gian quá sát nhau
                     create_date = latest_booking_time
            except ValueError:
                 # Fallback nếu ngày tháng có vấn đề
                 create_date = latest_booking_time - timedelta(minutes=random.randint(5, 60*24*BOOKING_WINDOW_DAYS))


            # 3. Chọn Khách hàng, Nhân viên, Giảm giá (tùy chọn)
            customer = random.choice(customers_data)
            employee = random.choice(employees_data)
            use_discount = random.random() < 0.3 # 30% cơ hội dùng giảm giá
            discount = None
            total_discount_amount = 0.0
            selected_discount_id = None

            if use_discount and discounts_data:
                # Tìm giảm giá hợp lệ tại thời điểm tạo hóa đơn
                valid_discounts = [d for d in discounts_data if d['startDate'] <= create_date.date() <= d['endDate'] and d['quantity'] > 0]
                if valid_discounts:
                    discount = random.choice(valid_discounts)
                    selected_discount_id = discount['id']


            # 4. "Mua" Vé cho Suất chiếu đã chọn
            num_tickets_to_buy = random.randint(1, 4) # Số vé mỗi lần mua
            invoice_tickets_list = [] # Vé thuộc hóa đơn này
            invoice_ticket_total_price = 0.0

            # Lấy ghế có sẵn trong phòng cho suất chiếu này
            room_seats = [s for s in seats_data if s['room_id'] == room_id]
            booked_seats_for_this_st = booked_seats_per_showtime.get(showtime_id, set())
            available_seats_in_room = [s for s in room_seats if s['status'] == 'Trống' and s['id'] not in booked_seats_for_this_st]

            if len(available_seats_in_room) < num_tickets_to_buy:
                 num_tickets_to_buy = len(available_seats_in_room) # Chỉ mua số ghế còn trống

            if num_tickets_to_buy == 0:
                # print(f"Bỏ qua hóa đơn: Không còn ghế trống cho suất chiếu {showtime_id}")
                continue # Bỏ qua vòng lặp hóa đơn này

            selected_seats_for_invoice = random.sample(available_seats_in_room, num_tickets_to_buy)

            for seat in selected_seats_for_invoice:
                ticket = {
                    'id': ticket_id_counter,
                    'bookingDate': create_date, # Nên giống createDate của hóa đơn
                    'price': seat['price'],
                    'qrCode': str(uuid.uuid4()),
                    'showtime_id': showtime_id,
                    'seat_id': seat['id'],
                    'invoice_id': invoice_id_counter # Sẽ được cập nhật chính xác sau
                }
                invoice_tickets_list.append(ticket)
                invoice_ticket_total_price += seat['price']
                booked_seats_for_this_st.add(seat['id']) # Đánh dấu ghế đã được đặt cho suất chiếu này
                ticket_id_counter += 1

            booked_seats_per_showtime[showtime_id] = booked_seats_for_this_st # Cập nhật lại dict theo dõi

            # 5. "Mua" Sản phẩm (tùy chọn)
            invoice_products_list = [] # Sản phẩm thuộc hóa đơn này
            invoice_product_total_price = 0.0
            if random.random() < 0.6: # 60% cơ hội mua thêm sản phẩm
                num_products_to_buy = random.randint(1, 3)
                # Chọn sản phẩm ngẫu nhiên từ danh sách sản phẩm còn hàng
                available_products = [p for p in products_data if p['quantity'] > 0]
                if available_products:
                    products_bought_this_invoice = random.sample(available_products, min(num_products_to_buy, len(available_products)))
                    for product_item in products_bought_this_invoice:
                         quantity_bought = random.randint(1, 2)
                         # Giảm số lượng tồn kho (quan trọng nếu cần dữ liệu chính xác)
                         # product_item['quantity'] -= quantity_bought # Chú ý: thay đổi trực tiếp dict gốc

                         usage = {
                             'id': product_usage_id_counter,
                             'quantity': quantity_bought,
                             'purchasePrice': product_item['price'], # Giá tại thời điểm mua
                             'product_id': product_item['id'],
                             'invoice_id': invoice_id_counter # Sẽ được cập nhật sau
                         }
                         invoice_products_list.append(usage)
                         invoice_product_total_price += product_item['price'] * quantity_bought
                         product_usage_id_counter += 1


            # 6. Tính toán Tổng cộng của Hóa đơn
            total_amount_before_discount = invoice_ticket_total_price + invoice_product_total_price

            if discount:
                if discount['type'] == "Phần trăm":
                    # Áp dụng % giảm giá cho tiền vé
                    total_discount_amount = round(invoice_ticket_total_price * (discount['discountValue'] / 100.0))
                elif discount['type'] == "Số tiền cố định":
                    # Giảm số tiền cố định, không vượt quá tổng tiền vé
                     total_discount_amount = round(min(invoice_ticket_total_price, discount['discountValue']))
                elif discount['type'] == "Đồng giá":
                     # Tính tổng tiền vé phải trả sau khi áp dụng đồng giá
                     discounted_ticket_price = discount['discountValue'] * len(invoice_tickets_list)
                     total_discount_amount = round(max(0, invoice_ticket_total_price - discounted_ticket_price)) # Số tiền được giảm
                else:
                     total_discount_amount = 0 # Các loại khác chưa xử lý

                # Giảm số lượng mã giảm giá còn lại
                discount['quantity'] -= 1 # Thay đổi trực tiếp dict gốc

            final_total_amount = round(total_amount_before_discount - total_discount_amount)
            # Đảm bảo tổng tiền không âm
            final_total_amount = max(0, final_total_amount)

            # 7. Tạo bản ghi Hóa đơn (Invoice)
            invoice = {
                'id': invoice_id_counter,
                'createDate': create_date, # Giữ datetime object
                'totalDiscount': round(total_discount_amount, -2), # Làm tròn VNĐ
                'totalAmount': round(final_total_amount, -2), # Làm tròn VNĐ
                'paymentMethod': random.choice(["Tiền mặt", "Thẻ tín dụng", "Chuyển khoản", "Ví điện tử"]),
                'qrCode': str(uuid.uuid4()), # QR code cho hóa đơn/biên nhận
                'status': random.choice(['Đã thanh toán', 'Chưa thanh toán', 'Đã hủy']),
                'note': fake.sentence(nb_words=10) if random.random() < 0.2 else None,
                'customer_id': customer['id'],
                'discount_id': selected_discount_id, # Có thể là None
                'employee_id': employee['id']
            }
            invoices_data.append(invoice)

            # 8. Cập nhật invoice_id cho Vé và ProductUsage
            for t in invoice_tickets_list:
                t['invoice_id'] = invoice['id']
                tickets_data.append(t)

            for pu in invoice_products_list:
                pu['invoice_id'] = invoice['id']
                product_usages_data.append(pu)

            invoice_id_counter += 1

else:
    print("Bỏ qua tạo Hóa đơn/Vé/Sử dụng sản phẩm do thiếu dữ liệu cần thiết.")


# --- Xuất dữ liệu ra các file CSV ---

print("\n--- Đang xuất dữ liệu ra các file CSV ---")

all_data = {
    "Cinema": cinemas_data,
    "Room": rooms_data,
    "Seat": seats_data,
    "Movie": movies_data, # Xuất cả dữ liệu phim đã xử lý
    "Customer": customers_data,
    "Employee": employees_data,
    "Discount": discounts_data,
    "Product": products_data,
    "ShowTime": showtimes_data,
    "Invoice": invoices_data,
    "Ticket": tickets_data,
    "ProductUsage": product_usages_data,
}

for table_name, data_list in all_data.items():
    if data_list: # Chỉ tạo file nếu có dữ liệu
        df = pd.DataFrame(data_list)
        # Chuyển đổi các cột ngày giờ sang định dạng chuỗi chuẩn trước khi lưu CSV nếu cần
        for col in df.columns:
            # Kiểm tra xem cột có chứa đối tượng datetime/date/time không
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                 # Định dạng DATETIME thành 'YYYY-MM-DD HH:MM:SS'
                 # Định dạng DATE thành 'YYYY-MM-DD'
                 # Định dạng TIME thành 'HH:MM:SS'
                 # Pandas thường xử lý tốt, nhưng có thể ép kiểu nếu muốn chắc chắn
                 try:
                     # Thử kiểm tra xem có phải chỉ chứa date không
                     if all(isinstance(x, pd.Timestamp) and x.time() == time(0, 0) for x in df[col].dropna()):
                          df[col] = df[col].dt.strftime('%Y-%m-%d')
                     # Thử kiểm tra xem có phải chỉ chứa time không (ít phổ biến hơn trong pandas)
                     # Cột time đã được xử lý thành object chứa time hoặc string
                     elif all(isinstance(x, time) for x in df[col].dropna()):
                          df[col] = df[col].astype(str) # Chuyển time object thành string
                     else: # Mặc định là datetime
                          df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                 except AttributeError: # Nếu cột không phải là kiểu datetime chuẩn của pandas (vd: đã là string)
                      pass
            elif all(isinstance(x, date) for x in df[col].dropna()):
                 df[col] = df[col].astype(str) # Chuyển date object thành string 'YYYY-MM-DD'
            elif all(isinstance(x, time) for x in df[col].dropna()):
                 df[col] = df[col].astype(str) # Chuyển time object thành string 'HH:MM:SS'


        filename = os.path.join(OUTPUT_DIR, f"{table_name}.csv")
        try:
            df.to_csv(filename, index=False, encoding='utf-8-sig') # utf-8-sig để Excel đọc tiếng Việt tốt
            print(f"Đã ghi dữ liệu bảng '{table_name}' vào file: {filename}")
        except Exception as e:
             print(f"Lỗi khi ghi file {filename}: {e}")
    else:
        print(f"Bỏ qua bảng '{table_name}' vì không có dữ liệu.")

print("\n--- Hoàn thành tạo dữ liệu CSV! ---")