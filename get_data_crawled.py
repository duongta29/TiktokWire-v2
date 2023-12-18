import json

def read_links_from_file(file_path):
    with open(file_path, 'r') as file:
        links = file.readlines()
    # Xóa các khoảng trắng và ký tự xuống dòng
    links = [link.strip() for link in links]
    return links

def shorten_links(tiktok_links):
    link_dict = {}
    
    for link in tiktok_links:
        # Tách tên trang và ID video từ liên kết
        page_name = link.split('@')[1].split('/')[0]
        video_id = link.split('/')[-1]
        
        # Kiểm tra xem trang đã tồn tại trong từ điển chưa
        if page_name in link_dict:
            # Nếu đã tồn tại, thêm ID video vào danh sách
            link_dict[page_name].append(video_id)
        else:
            # Nếu chưa tồn tại, tạo một cặp key-value mới
            link_dict[page_name] = [video_id]
    
    return link_dict
# Đường dẫn đến tệp dữ liệu
def save_dictionary_to_json(dictionary, file_path):
    with open(file_path, 'w') as file:
        json.dump(dictionary, file)
            
file_path_utils = 'dataCrawled.txt'
file_path = "data_crawled.json"

# Đọc các liên kết từ tệp dữ liệu
tiktok_links = read_links_from_file(file_path_utils)

# Tạo từ điển từ các liên kết TikTok
link_dictionary = shorten_links(tiktok_links)

save_dictionary_to_json(link_dictionary, file_path)
# In kết quả
print(link_dictionary)