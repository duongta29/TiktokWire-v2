import json
import os

def update_file_crawled(page_name, video_id):
    folder_path = "dataCrawled"
    file_path = os.path.join(folder_path, f"{page_name}.txt")

    # Kiểm tra xem tệp tin đã tồn tại hay chưa
    if not os.path.exists(file_path):
        # Tạo tệp tin mới nếu chưa tồn tại
        with open(file_path, "w") as file:
            file.write(video_id + "\n")
    else:
        # Mở tệp tin và thêm video_id vào cuối tệp tin
        with open(file_path, "a") as file:
            file.write(video_id + "\n")

def write_post_to_file(post):
    with open("result.txt", "a", encoding="utf-8") as file:
        file.write(f"{str(post)}\n")
        if post.is_valid:
            file.write("🇧🇷" * 50 + "\n")
        else:
            file.write("🎈" * 50 + "\n")