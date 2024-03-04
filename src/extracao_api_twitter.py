from datetime import datetime, timedelta
import requests
import json

# montando url
TIME_STAMP = "%Y-%m-%dT%H:%M:%S.00Z"

end_time = datetime.now().strftime(TIME_STAMP)
start_time = (datetime.now() + timedelta(-1)).date().strftime(TIME_STAMP)
query = 'data science'

tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

# requisicao
response = requests.request('GET', url_raw)

# imprimir json
json_response = response.json()

print(json.dumps(json_response, indent=4, sort_keys=True))

# paginate
while 'next_token' in json_response.get('meta', {}):
    next_token = json_response['meta']['next_token']
    url = f'{url_raw}&next_token={next_token}'
    response = requests.request('GET', url)

    json_response = response.json()
    print(json.dumps(json_response, indent=4, sort_keys=True))