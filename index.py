import requests
import datetime

# asking for new token
url = "https://bankaccountdata.gocardless.com/api/v2/token/new/"

headers = {
    "accept": "application/json",
    "Content-Type": "application/json"
}
data = {
    "secret_id": "7b62822d-f18e-453a-bc96-ee20d9de1f48",  # Replace "your_secret_id" with your actual secret_id
    "secret_key": "e2a8dbba8b6cb8b8e3e4bc7b640540a1c1f4d45bbc36fb8e3bcd0ab4e0151f21b893c29ff4850ff09c983ceb9e1927070deff919f7bf8d4d4e1a10df141de14e"  # Replace "your_secret_key" with your actual secret_key
}

response = requests.post(url, headers=headers, json=data)

# response_data = response.json()

access_token = response.json()['access']
refresh_token = response.json()['refresh']

print(f'Access_token: {access_token}')
print(f'Refresh_token: {refresh_token}')


# auth token to gain access
url = "https://bankaccountdata.gocardless.com/api/v2/token/new/"

headers = {
    "accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f'Bearer {access_token}'
}

data_url = "https://bankaccountdata.gocardless.com/api/v2/institutions/?country=gb"

response = requests.get(data_url, headers=headers)

# print(response.status_code)
# print(response.json())



# ASKING FOR TRANSACTIONS
id_nr = 'acc_0000ALJzaprw4rbOjUGTon'

url = f"https://bankaccountdata.gocardless.com/api/v2/accounts/{id_nr}/transactions/"

headers = {
    "accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": f'Bearer {access_token}'
}

response = requests.get(url, headers=headers)

print(response.status_code)
print(response.json())