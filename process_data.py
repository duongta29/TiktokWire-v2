import json

def update_json_file(file_path, new_link):
    # Đọc tệp JSON hiện có
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Tách tên trang và ID video từ liên kết mới
    page_name = new_link.split('@')[1].split('/')[0]
    video_id = new_link.split('/')[-1]

    # Kiểm tra xem trang đã tồn tại trong từ điển chưa
    if page_name in data:
        # Nếu đã tồn tại, thêm ID video vào danh sách
        data[page_name].append(video_id)
    else:
        # Nếu chưa tồn tại, tạo một cặp key-value mới
        data[page_name] = [video_id]

    # Ghi từ điển đã cập nhật vào tệp JSON
    with open(file_path, 'w') as file:
        json.dump(data, file)

def write_post_to_file(post):
    with open("result.txt", "a", encoding="utf-8") as file:
        file.write(f"{str(post)}\n")
        if post.is_valid:
            file.write("🇧🇷" * 50 + "\n")
        else:
            file.write("🎈" * 50 + "\n")