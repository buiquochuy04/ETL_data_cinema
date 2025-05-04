import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import time
import pandas as pd
import os

def get_info(soup):
    movie_data = {
    'Tên phim': '',
    'Poster':'',
    'Đạo diễn': '',
    'Diễn viên': '',
    'Thể loại': '', # Thêm các key bạn mong đợi khác
    'Khởi chiếu': '',
    'Thời lượng': '',
    'Ngôn ngữ': '',
    'Rated': '',     # Key cho trường có thể thiếu
    'Chi tiết': '',
    'Trailer': ''
    }
    info_divs = soup.find_all('div', class_='movie-info')
    for div in info_divs:
        label_tag = div.find('label')
        value_tag = div.find('div', class_='std')

        if label_tag and value_tag:
            # Lấy key, làm sạch (bỏ dấu :, bỏ khoảng trắng thừa)
            key = label_tag.get_text(strip=True).replace(':', '')
            # Lấy value, làm sạch (bỏ khoảng trắng thừa)
            value = value_tag.get_text(strip=True)

            # Chỉ cập nhật nếu key này nằm trong các key mong đợi của chúng ta
            if key in movie_data:
                movie_data[key] = value
    info_div = soup.find_all('div', class_='tab-content')
    for div in info_div:
        label_tag = div.find('h2')
        value_tag = div.find('div', class_='std')

        if label_tag and value_tag:
            # Lấy key, làm sạch (bỏ dấu :, bỏ khoảng trắng thừa)
            key = label_tag.get_text(strip=True).replace(':', '')
            # Lấy value, làm sạch (bỏ khoảng trắng thừa)
            value = value_tag.get_text(strip=True)

            # Chỉ cập nhật nếu key này nằm trong các key mong đợi của chúng ta
            if key in movie_data:
                movie_data[key] = value
    info_di = soup.find_all('div', class_='movie-rated-web')
    for div in info_di:
        label_tag = div.find('label')
        value_tag = div.find('div', class_='std')

        if label_tag and value_tag:
            # Lấy key, làm sạch (bỏ dấu :, bỏ khoảng trắng thừa)
            key = label_tag.get_text(strip=True).replace(':', '')
            # Lấy value, làm sạch (bỏ khoảng trắng thừa)
            value = value_tag.get_text(strip=True)

            # Chỉ cập nhật nếu key này nằm trong các key mong đợi của chúng ta
            if key in movie_data:
                movie_data[key] = value
    info_name = soup.find('div', class_='product-name')
    if info_name:
        name = info_name.find('span', class_ ='h1').get_text(strip=True)
        movie_data['Tên phim'] = name
    poster_container = soup.find('div', class_ = 'product-image-gallery')
    if poster_container:
        img_tag = poster_container.find('img')
        if img_tag and img_tag.get('src'):
            movie_data['Poster'] = img_tag.get('src')
    trailer_container = soup.find('div', class_='product_view_trailer')
    if trailer_container:
        iframe_tag = trailer_container.find('iframe')
        # Kiểm tra iframe tồn tại VÀ có thuộc tính 'src'
        if iframe_tag and iframe_tag.get('src'):
            src_url = iframe_tag.get('src')
            # Đảm bảo URL có scheme (http/https) nếu nó bắt đầu bằng //
            if src_url.startswith('//'):
                src_url = 'https:' + src_url
            movie_data['Trailer'] = src_url
    return movie_data

URL = "https://www.cgv.vn/default/"

# 1. Cấu hình Chrome chạy ẩn
options = Options()
options.add_argument("--headless")  # Xóa dòng này nếu muốn thấy trình duyệt
driver = webdriver.Chrome(options=options)

# 2. Mở trang CGV - phim đang chiếu
driver.get(URL)
time.sleep(5)  # Chờ JS render xong

# 3. Dùng BeautifulSoup để phân tích nội dung HTML sau khi JS render xong
soup = BeautifulSoup(driver.page_source, "html.parser")

category = soup.select('li.level1[class*="nav-1-"] a.level1')

phim_list = []

dangchieu = []

for cate in category:
  link = cate.get("href")
  driver.get(link)
  time.sleep(2)
  phim = BeautifulSoup(driver.page_source, "html.parser")
  tong_phim = phim.select(".product-images a.product-image")
  phim_list += tong_phim
  dangchieu.append(len(tong_phim))

div_std = []

html = []

for phim in phim_list:
    title = phim.text.strip()
    link = phim.get("href")
    driver.get(link)
    time.sleep(2)  # đợi trang load
    detail_soup = BeautifulSoup(driver.page_source, "html.parser")
    html.append(detail_soup)

info = []

for h in html:
  phim_info = get_info(h)
  info.append(phim_info)

df = pd.DataFrame(info)

num_a = dangchieu[0]  # Số hàng đầu tiên gán giá trị 'a'
new_column_name = 'Trạng thái' # Tên cột mới

# Gán 'a' cho các hàng từ chỉ số 0 đến num_a - 1
# Lưu ý: .loc[start:end] bao gồm cả start và end khi dùng label index
#        Nhưng với default integer index, nó hoạt động giống iloc khi cắt đến vị trí
#        Để chắc chắn, ta cắt đến num_a - 1
if num_a > 0:
    df.loc[0:num_a-1, new_column_name] = 'Đang chiếu'

# Gán 'b' cho các hàng từ chỉ số num_a trở đi
df.loc[num_a:, new_column_name] = 'Sắp chiếu'

output_folder = 'D:\ETL\data'
file_name = 'movies.csv'
relative_path = os.path.join(output_folder, file_name)
df.index.name = 'id'
df.to_csv(relative_path, encoding='utf-8-sig')
