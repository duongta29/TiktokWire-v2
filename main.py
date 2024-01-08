from crawl import CrawlManage
import threading
import json
import time

def check_list_config(running_ids):
    threads = {}
    lock = threading.Lock()

    while True:
        with open("dev_config.json", "r", encoding="utf-8") as list_config:
            list_config = json.load(list_config)
            list_config_id = []
            for config in list_config:
                config_id = config.get("id")
                list_config_id.append(config_id)
                if config_id not in running_ids:
                    crawl = CrawlManage(config=config)
                    stop_event = threading.Event()
                    crawl.stop_event = stop_event  # Gán stop_event cho crawl
                    new_thread = threading.Thread(target=crawl.run)
                    new_thread.start()
                    running_ids.append(config_id)
                    with lock:
                        threads[config_id] = (new_thread, stop_event)
            threads_to_remove = []
            for id, (thread, stop_event) in threads.items():
                if id not in list_config_id:
                    # Gửi tín hiệu dừng cho luồng
                    stop_event.set()
                    thread.join(timeout=0)
                    threads_to_remove.append(id)
            for id in threads_to_remove:
                # Xóa luồng khỏi danh sách
                with lock:
                    threads.pop(id)
        time.sleep(10)
                
        



if __name__ == '__main__':
    check_list_config(running_ids=[])
    