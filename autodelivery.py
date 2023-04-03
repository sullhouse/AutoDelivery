import requests
import json
from autodelivery_config import *
from aosapiconnection import *
from ftpconnection import *
import datetime
import random

aos_api_connection = get_aos_api_connection()
#print(token)

# Prep deliverydata output for csv, and create a place to log contents for contents csv
primary_deliverydata = []
primary_delivery_csv_header = 'Production System ID,Production System Name,Delivered Date,Advertiser ID,Advertiser Name,Order ID,Order Name,Line Item ID,Line Item Name,Unit Type 1,Delivered Quantity 1,Unit Type 2,Delivered Quantity 2,Start Date,End Date,Unit Cost,Cost Method,Associated Sales Line Item ID'
#print(delivery_csv_header)
primary_deliverydata.append(primary_delivery_csv_header)

third_party_deliverydata = []
third_party_delivery_csv_header = 'Production System ID,Production System Name,Delivered Date,Advertiser ID,Advertiser Name,Order ID,Order Name,Line Item ID,Line Item Name,Unit Type 1,Delivered Quantity 1,Unit Type 2,Delivered Quantity 2,Start Date,End Date,Associated Production System ID,Associated Production System Name,Associated Line Item ID,Associated Line Item Name'
third_party_deliverydata.append(third_party_delivery_csv_header)

contents = []
contents_csv_header = 'Primary Production System ID,Primary Production System Name,Advertiser ID,Advertiser Name,Order ID,Order Name,Start Date,End Date,Primary Line Item Count,Third Party Line Item Count'
print(contents_csv_header)
contents.append(contents_csv_header)

# Get workstream data
page_num=0
workstream_search_filter = {"startDate":date_range_start_to_process,"endDate":date_range_end_to_process,"externalSystemIds":external_system_ids_to_process}
if deal_id_to_process > 0: workstream_search_filter.update({"dealSequenceId": deal_id_to_process})
earliest_delivery_start_date = datetime.datetime.strptime(earliest_delivery_start_date, "%Y-%m-%d").date()
latest_delivery_end_date = datetime.datetime.strptime(latest_delivery_end_date, "%Y-%m-%d").date()

while True:
    page_num += 1
    url = 'https://' + aos_api_connection['apiurl'] + '/orders/v1/' + aos_api_connection['apikey'] + '/workstreams/_search'
    workstream_search_filter.update({"pageNumber": page_num, "pageSize": 100})
    workstreamresponse = requests.post(url, json=workstream_search_filter, headers=aos_api_connection['headers'])
    #print(f"Status Code: {workstreamresponse.status_code}, Response: {workstreamresponse.json()}")
    
    # If there are no workstreams, break from the loop
    if "workstreams" not in workstreamresponse.text:
        break
    
    # If there are workstreams, process them
    workstreamjson = json.loads(workstreamresponse.text)
    workstreams = workstreamjson["workstreams"]
    for workstream in workstreams:
        outputdata = []
        # Get line item data
        url = 'https://' + aos_api_connection['apiurl'] + '/orders/v1/' + aos_api_connection['apikey'] + '/lines/_search'
        lineitemresponse = requests.post(url, json={
        "workstreamId":workstream['id'],
        "performanceView":True
        }, headers=aos_api_connection['headers'])
        #print(f"Status Code: {lineitemresponse.status_code}, Response: {lineitemresponse.json()}")
        lineitemjson = json.loads(lineitemresponse.text)
        lineitems = lineitemjson["lineItems"]
        for lineitem in lineitems:
            if 'everPushCompleted' in lineitem and lineitem['everPushCompleted'] == True and lineitem['unitType']['name'].lower() in unit_types_to_process and lineitem['pushQuantity'] > 10:
                # Get line item details for delivery file
                outputdatalineitem = {
                    'start_date': lineitem['startDate'],
                    'end_date': lineitem['endDate'],
                    'unit_type': lineitem['unitType']['name'],
                    'quantity': lineitem['pushQuantity'],
                    'unit_cost': lineitem['netUnitCost'],
                    'cost_method': lineitem['salesCostMethod']['name'],
                    'deal_line_id': lineitem['dealLineSequenceId'],
                    'external_system_id': workstream['externalSystem']['id'],
                    'external_system_name': f'"{workstream["externalSystem"]["name"]}"' if ',' in workstream['externalSystem']['name'] else workstream['externalSystem']['name'],
                    'external_order_id': lineitem['externalAds'][0]['externalOrder']['externalId'],
                    'external_advertiser_id': lineitem['externalAds'][0]['externalAdvertiser']['id'],
                    'external_advertiser_name': f'"{lineitem["externalAds"][0]["externalAdvertiser"]["name"]}"' if ',' in lineitem['externalAds'][0]['externalAdvertiser']['name'] else lineitem['externalAds'][0]['externalAdvertiser']['name'],
                    'external_line_id': lineitem['externalAds'][0]['externalId'],
                    'external_order_name': f'"{workstream["orderName"]}"' if ',' in workstream['orderName'] else workstream['orderName'],
                    'external_line_name': f'"{lineitem["name"]}"' if ',' in lineitem['name'] else lineitem['name'],
                }
                if generate_third_party_delivery and 'thirdPartyBillableAdServer' in lineitem:
                    outputdatalineitem['third_party_system_id'] = lineitem['thirdPartyBillableAdServer']['id']
                    outputdatalineitem['third_party_system_name'] = lineitem['thirdPartyBillableAdServer']['name']
                outputdata.append(outputdatalineitem)
                #print(outputdatalineitem)

        # Generate delivery data for each line item
        primary_line_item_count = 0
        third_party_line_item_count = 0

        for outputdatalineitem in outputdata:
            primary_line_item_count += 1
            line_start_date = datetime.datetime.strptime(outputdatalineitem['start_date'], "%Y-%m-%d").date() 
            delivery_start_date = max(earliest_delivery_start_date, line_start_date)
            line_end_date = datetime.datetime.strptime(outputdatalineitem['end_date'], "%Y-%m-%d").date()
            delivery_end_date = min(latest_delivery_end_date, line_end_date)
            delta = datetime.timedelta(days=1)
            days_in_line_item = (line_end_date - line_start_date).days + 1
            quantity_per_day = outputdatalineitem['quantity'] / days_in_line_item
            if generate_primary_delivery:
                row_date = delivery_start_date
                while row_date <= delivery_end_date:
                    rand_percent = random.uniform(-0.05, 0.05)
                    adjusted_quantity = round(quantity_per_day * (1 + rand_percent))
                    delivery_csv_row = (
                        outputdatalineitem['external_system_id'] + ',' +
                        outputdatalineitem['external_system_name'] + ',' +
                        str(row_date) + ',' +
                        outputdatalineitem['external_advertiser_id'] + ',' +
                        outputdatalineitem['external_advertiser_name'] + ',' +
                        outputdatalineitem['external_order_id'] + ',' +
                        outputdatalineitem['external_order_name'] + ',' +
                        outputdatalineitem['external_line_id'] + ',' +
                        outputdatalineitem['external_line_name'] + ',' +
                        outputdatalineitem['unit_type'] + ',' +
                        str(adjusted_quantity) + ',' +
                        'clicks,' + str(round(adjusted_quantity * .03)) + ',' +
                        outputdatalineitem['start_date'] + ',' +
                        outputdatalineitem['end_date'] + ',' +        
                        str(outputdatalineitem['unit_cost']) + ',' +
                        outputdatalineitem['cost_method'] + ','
                    )
                    primary_deliverydata.append(delivery_csv_row)
                    row_date += delta

            if generate_third_party_delivery and 'third_party_system_id' in outputdatalineitem:
                third_party_line_item_count += 1
                row_date = delivery_start_date
                while row_date <= delivery_end_date:
                    rand_percent = random.uniform(-0.03, 0.00)
                    third_party_adjusted_quantity = round(adjusted_quantity * (1 + rand_percent))
                    third_party_delivery_csv_row = (
                        outputdatalineitem['third_party_system_id'] + ',' +
                        outputdatalineitem['third_party_system_name'] + ',' +
                        str(row_date) + ',' +
                        outputdatalineitem['external_advertiser_id'] + '_tp,' +
                        outputdatalineitem['external_advertiser_name'] + ',' +
                        outputdatalineitem['external_order_id'] + '_tp,' +
                        outputdatalineitem['external_order_name'] + ',' +
                        outputdatalineitem['external_line_id'] + '_tp,' +
                        outputdatalineitem['external_line_name'] + '_' + outputdatalineitem['third_party_system_name'] + ',' +
                        outputdatalineitem['unit_type'] + ',' +
                        str(third_party_adjusted_quantity) + ',' +
                        'clicks,' + str(round(third_party_adjusted_quantity * .03)) + ',' +
                        outputdatalineitem['start_date'] + ',' +
                        outputdatalineitem['end_date'] + ',' +
                        outputdatalineitem['external_system_id'] + ',' +
                        outputdatalineitem['external_system_name'] + ',' +
                        outputdatalineitem['external_line_id'] + ',' +
                        outputdatalineitem['external_line_name']
                    )
                    third_party_deliverydata.append(third_party_delivery_csv_row)
                    row_date += delta

        # Log the contents
        if bool(outputdata): 
            contents_csv_row = (
                outputdatalineitem['external_system_id'] + ',' +
                outputdatalineitem['external_system_name'] + ',' +
                outputdatalineitem['external_advertiser_id'] + ',' +
                outputdatalineitem['external_advertiser_name'] + ',' +
                outputdatalineitem['external_order_id'] + ',' +
                outputdatalineitem['external_order_name'] + ',' +
                outputdatalineitem['start_date'] + ',' +
                outputdatalineitem['end_date'] + ',' +        
                str(primary_line_item_count) + ',' +
                str(third_party_line_item_count)
            )
            print(contents_csv_row)
            contents.append(contents_csv_row)

# Upload data as csv to FTP
suffix = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
if generate_primary_delivery: primary_delivery_filename = upload_file_to_ftp('Primary', suffix, primary_deliverydata, delete_from_local_after_upload)
if generate_third_party_delivery: third_party_delivery_filename = upload_file_to_ftp('ThirdParty', suffix, third_party_deliverydata, delete_from_local_after_upload)
if generate_primary_delivery or generate_third_party_delivery: contents_filename = upload_file_to_ftp('Contents', suffix, contents, delete_from_local_after_upload)

# Trigger delivery pulls via API
if call_aos_data_pull and generate_primary_delivery: trigger_primary_delivery_pull(aos_api_connection, delivery_source_id, primary_delivery_filename)
if call_aos_data_pull and generate_third_party_delivery: trigger_third_party_delivery_pull(aos_api_connection, delivery_source_id, third_party_delivery_filename)