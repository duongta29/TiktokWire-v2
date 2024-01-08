from crawl import CrawlManage
import multiprocessing
import json
import time

def check_config(config_id, config):
    crawl = CrawlManage(config=config)
    crawl.run()

def check_list_config(running_ids):
    processes = []
    while True:
        with open("dev_config.json", "r", encoding="utf-8") as list_config:
            list_config = json.load(list_config)
            list_config_id = []
            for config in list_config:
                config_id = config.get("id")
                list_config_id.append(config_id)
                if config_id not in running_ids:
                    process = multiprocessing.Process(target=check_config, args=(config_id, config))
                    process.start()
                    running_ids.append(config_id)
                    processes.append(process)
            processes_to_remove = []
            for process in processes:
                if not process.is_alive():
                    process.join(timeout=0)
                    processes_to_remove.append(process)
            for process in processes_to_remove:
                processes.remove(process)
        time.sleep(10)

if __name__ == '__main__':
    check_list_config(running_ids=[])