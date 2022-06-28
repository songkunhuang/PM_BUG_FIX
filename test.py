import requests, json

data = {
    'content_id': 1656379055926647051703,
    'name': 'lily',
    'age': 11,
    'birthplace': 'san',
    'grade': 123
}
url = 'http://127.0.0.1:19998/pm/contentFix/'

response = requests.post(url, data=json.dumps(data))

print(json.dumps(json.loads(response.text)))