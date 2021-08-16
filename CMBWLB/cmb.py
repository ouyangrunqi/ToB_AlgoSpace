import requests
import json


req_url = 'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_control/list?algo_type_version_id=274'

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
}
res = requests.get(req_url, headers=headers)
print(res.text)


res_json = json.loads(res.text)
for da in res_json['data']:
    for k,v in da.items():
        print(k,v)

# print(res_json['data'])
# for id in res_json['data']['id']:
#     print(res_json['data']['id'])
#     print(id)

