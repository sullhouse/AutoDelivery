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
    aos_api_connection['tenantname'] = creds['tenantname']
    aos_api_connection['headers'] = {"Authorization": "Bearer " + aos_api_connection['token']}

    return aos_api_connection

def get_workstreams_from_aos_api(aos_api_connection, page_num, date_range_start_to_process, date_range_end_to_process, external_system_ids_to_process, deal_id_to_process):
    workstream_search_filter = {"startDate":date_range_start_to_process,"endDate":date_range_end_to_process,"externalSystemIds":external_system_ids_to_process}
    if deal_id_to_process > 0: workstream_search_filter.update({"dealSequenceId": deal_id_to_process})

    url = 'https://' + aos_api_connection['apiurl'] + '/orders/v1/' + aos_api_connection['apikey'] + '/workstreams/_search'
    workstream_search_filter.update({"pageNumber": page_num, "pageSize": 100})
    response = requests.post(url, json=workstream_search_filter, headers=aos_api_connection['headers'])
    #print(f"Status Code: {workstreamresponse.status_code}, Response: {workstreamresponse.json()}")
    
    # If there are workstreams return them
    if "workstreams" in response.text:
        workstreams = json.loads(response.text)["workstreams"]
    else:
        workstreams = []

    return workstreams

def get_lineitems_from_aos_api(aos_api_connection, workstream_id):
    url = 'https://' + aos_api_connection['apiurl'] + '/orders/v1/' + aos_api_connection['apikey'] + '/lines/_search'
    response = requests.post(url, json={
    "workstreamId":workstream_id,
    "performanceView":True
    }, headers=aos_api_connection['headers'])
    #print(f"Status Code: {lineitemresponse.status_code}, Response: {lineitemresponse.json()}")
    lineitems = json.loads(response.text)["lineItems"]

    return lineitems

def get_deal_header_from_aos_api(aos_api_connection, deal_id):
    url = 'https://' + aos_api_connection['apiurl'] + '/unifiedplanner/v1/' + aos_api_connection['apikey'] + '/plans/' + str(deal_id)
    response = requests.get(url, headers=aos_api_connection['headers'])
    #print(f"Status Code: {dealresponse.status_code}, Response: {dealresponse.json()}")
    return json.loads(response.text)

def get_deal_lineitems_from_aos_api(aos_api_connection, deal_id):
    url = 'https://' + aos_api_connection['apiurl'] + '/unifiedplanner/v1/' + aos_api_connection['apikey'] + '/plans/' + str(deal_id) + '/workspace/digital/lines/_search'
    response = requests.post(url, json={}, headers=aos_api_connection['headers'])
    return json.loads(response.text)

def get_external_systems_from_aos_api(aos_api_connection):
    url = 'https://' + aos_api_connection['apiurl'] + '/mdm/v1/' + aos_api_connection['apikey'] + '/psDefinition'
    response = requests.get(url, headers=aos_api_connection['headers'])
    return json.loads(response.text)

def get_cost_methods_from_aos_api(aos_api_connection):
    url = 'https://' + aos_api_connection['apiurl'] + '/mdm/v1/' + aos_api_connection['apikey'] + '/costmethod'
    response = requests.get(url, headers=aos_api_connection['headers'])
    return json.loads(response.text)

def get_unit_types_from_aos_api(aos_api_connection):
    url = 'https://' + aos_api_connection['apiurl'] + '/mdm/v1/' + aos_api_connection['apikey'] + '/unittype'
    response = requests.get(url, headers=aos_api_connection['headers'])
    return json.loads(response.text)

def get_products_from_aos_api(aos_api_connection):
    url = 'https://' + aos_api_connection['apiurl'] + '/products/v1/' + aos_api_connection['apikey'] + '/productList?pageSize=10000'
    response = requests.get(url, headers=aos_api_connection['headers'])
    return json.loads(response.text)

def get_packages_from_aos_api(aos_api_connection):
    url = 'https://' + aos_api_connection['apiurl'] + '/products/v1/' + aos_api_connection['apikey'] + '/productgroups/_search'
    response = requests.post(url, json={"fetchTemplatizedOnly": True}, headers=aos_api_connection['headers'])
    return json.loads(response.text)

def trigger_primary_delivery_pull_aos_api(aos_api_connection, delivery_source_id, filename):
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

def trigger_third_party_delivery_pull_aos_api(aos_api_connection, delivery_source_id, filename):
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
