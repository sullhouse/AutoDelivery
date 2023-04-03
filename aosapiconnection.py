import json
import requests

def get_aos_api_connection():
    aos_api_connection = {}
    creds_file = 'aosapicredentials.json'
    with open(creds_file) as f:
        creds = json.load(f)
    url = 'https://' + creds['authurl'] + '/mayiservice/tenant/' + creds['tenantname']
    authresponse = requests.post(url, json={
        "expiration": creds['expiration'],
        "password": creds['apipassword'],
        "userId": creds['apiusername'],
        "apiKey": creds['apikey']
    })
    authjson = json.loads(authresponse.text)
    aos_api_connection['token'] = authjson['token']
    aos_api_connection['apiurl'] = creds['apiurl']
    aos_api_connection['apikey'] = creds['apikey']
    aos_api_connection['headers'] = {"Authorization": "Bearer " + aos_api_connection['token']}

    return aos_api_connection

def trigger_primary_delivery_pull(aos_api_connection, delivery_source_id, filename):
     # Trigger primary delivery pull via API
    url = 'https://' + aos_api_connection['apiurl'] + '/ingress/v2/' + aos_api_connection['apikey'] + '/ingest/inlinePayload'
    response = requests.post(url, json={
            "callBackUrl": "",
            "platform": "Digital",
            "entityDetail": {
                "entitySource": "PAYLOAD",
                "entityType": "DeliveryPull"
            },
            "externalSystemId": "",
            "externalSystemName": "Adserver",
            "payload": "{\"startDate\":\"2022-11-25T06:41:00.0\",\"productionSystemId\":\"" + delivery_source_id + "\",\"file\":\"" + filename + "\"}",
            "routingDetail": {
                "sourceSystemName": {
                    "extSystemId": "",
                    "extSystemName": "AOS"
                },
                "targetSystemName": {
                    "extSystemId": "",
                    "extSystemName": "Adserver"
                }
            },
            "schemaDetail": {
                "keyFields": []
            }
        }, headers=aos_api_connection['headers'])
    print(f"Status Code: {response.status_code}, Response: {response.json()}")

def trigger_third_party_delivery_pull(aos_api_connection, delivery_source_id, filename):
    url = 'https://' + aos_api_connection['apiurl'] + '/ingress/v2/' + aos_api_connection['apikey'] + '/ingest/inlinePayload'
    response = requests.post(url, json={
            "callBackUrl": "",
            "platform": "Digital",
            "entityDetail": {
                "entitySource": "PAYLOAD",
                "entityType": "ThirdPartyDeliveryPull"
            },
            "externalSystemId": "",
            "externalSystemName": "Adserver",
            "payload": "{\"startDate\":\"2022-11-25T06:41:00.0\",\"productionSystemId\":\"" + delivery_source_id + "\",\"file\":\"" + filename + "\"}",
            "routingDetail": {
                "sourceSystemName": {
                    "extSystemId": "",
                    "extSystemName": "AOS"
                },
                "targetSystemName": {
                    "extSystemId": "",
                    "extSystemName": "Adserver"
                }
            },
            "schemaDetail": {
                "keyFields": []
            }
        }, headers=aos_api_connection['headers'])
    print(f"Status Code: {response.status_code}, Response: {response.json()}")
