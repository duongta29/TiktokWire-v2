from elasticsearch import Elasticsearch
from datetime import datetime

def get_link_es(type_list,gte,lte):
    link = []
    for _type in type_list:
        es = Elasticsearch(["http://192.168.143.54:9200"])
        body1 ={
        "query": {
            "bool": {
            "must": [
                {
                "term": {
                    "type.keyword": f"{_type}"
                }
                },
                {
                "range": {
                    "created_time": {
                    "gte": f"{gte}"
                    , "lte": f"{lte}"
                    }
                }
                }
            ]
            }
        },
        "sort": [
            {
            "_id": "desc"
            }
        ]
        }
        date_format = "%m/%d/%Y %H:%M:%S"
        # Lấy kết quả đầu tiên
        result = es.search(index="osint_posts", body=body1)
        dataFramse_Log = []
        for result_source in result['hits']['hits']:
            dataFramse_Log.append(result_source)
        # Lấy kết quả tiếp theo bằng cách sử dụng search_after
        while len(result["hits"]["hits"]) > 0:
            last_hit = result["hits"]["hits"][-1]
            body1["search_after"] = [last_hit["_id"]]
            result = es.search(index="osint_posts", body=body1)
            for result_source in result['hits']['hits']:
                dataFramse_Log.append(result_source)
        
        for item in dataFramse_Log:
                _type=_type.replace(" ","_")
                # with open(f"data_{_type}.txt","a") as file:
                #     file.write(f"{item['_source']['link']}|{item['_source']['like']}|{item['_source']['love']}|{item['_source']['comment']}|{item['_source']['share']}\n")
                link.append(item['_source']['link'])
    return link
                    
        
# if __name__ == "__main__":
#     type_list=['tiktok video']
#     # format : mm/dd/yyyy hh:mm:ss
#     gte='12/20/2023 17:00:00'
#     lte='12/21/2023 17:00:00'
#     link = get_link_es(type_list,gte,lte)
#     print(link)