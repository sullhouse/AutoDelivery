import requests
import json
from autodelivery_config import *
from aosapiconnection import *
from ftpconnection import *
from deliverydata import *
import datetime
import random

aos_api_connection = get_aos_api_connection()
#print(token)

# Prep deliverydata output for csv, and create a place to log contents for contents csv
primary_deliverydata = []
primary_deliverydata.append(primary_delivery_csv_header)

third_party_deliverydata = []
third_party_delivery_csv_header = 'Production System ID,Production System Name,Delivered Date,Advertiser ID,Advertiser Name,Order ID,Order Name,Line Item ID,Line Item Name,Unit Type 1,Delivered Quantity 1,Unit Type 2,Delivered Quantity 2,Start Date,End Date,Associated Production System ID,Associated Production System Name,Associated Line Item ID,Associated Line Item Name'
third_party_deliverydata.append(third_party_delivery_csv_header)

contents = []
contents_csv_header = 'Primary Production System ID,Primary Production System Name,Advertiser ID,Advertiser Name,Order ID,Order Name,Start Date,End Date,Primary Line Item Count,Third Party Line Item Count'
print(contents_csv_header)
contents.append(contents_csv_header)

# Get dates of delivery data to process
if previous_days > 0:
    earliest_delivery_start_date = datetime.date.today() - datetime.timedelta(days=previous_days)
    latest_delivery_end_date = datetime.date.today() - datetime.timedelta(days=1)
else:
    earliest_delivery_start_date = datetime.datetime.strptime(earliest_delivery_start_date, "%Y-%m-%d").date()
    latest_delivery_end_date = datetime.datetime.strptime(latest_delivery_end_date, "%Y-%m-%d").date()

page_num=0
while True:
    page_num += 1
    
    # Get workstream data
    workstreams = get_workstreams(aos_api_connection, page_num, date_range_start_to_process, date_range_end_to_process, external_system_ids_to_process, deal_id_to_process)
    if len(workstreams)==0: break
    for workstream in workstreams:
        deliverydata = []

        # Get line item data in this workstream
        lineitems = get_lineitems(aos_api_connection, workstream['id'])
        for lineitem in lineitems:
            deliverylineitem = get_deliverylineitem(lineitem, unit_types_to_process, workstream, generate_third_party_delivery)
            if deliverylineitem is not None:
                deliverydata.append(deliverylineitem)

        # Generate delivery data for each line item
        primary_line_item_count = 0
        third_party_line_item_count = 0

        for deliverylineitem in deliverydata:
            dates_quantities = get_dates_quantities(deliverylineitem, earliest_delivery_start_date, latest_delivery_end_date)
            if generate_primary_delivery: primary_line_item_count += 1
            if generate_third_party_delivery and 'third_party_system_id' in deliverylineitem: third_party_line_item_count +=1

            for date_quantity in dates_quantities:
                if generate_primary_delivery: primary_deliverydata.append(get_primary_delivery_csv_row(deliverylineitem, date_quantity['delivery_date'], date_quantity['primary_quantity']))
                if generate_third_party_delivery and 'third_party_system_id' in deliverylineitem: third_party_deliverydata.append(get_third_party_delivery_csv_row(deliverylineitem, date_quantity['delivery_date'], date_quantity['third_party_quantity']))
                    
        # Log the contents
        if bool(deliverydata): 
            contents.append(get_contents_csv_row(deliverylineitem, primary_line_item_count, third_party_line_item_count))
            print(contents[-1])

# Upload data as csv to AOS FTP
suffix = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
if generate_primary_delivery: primary_delivery_filename = upload_file_to_ftp('Primary', suffix, primary_deliverydata, delete_from_local_after_upload, 'aos_ftp')
if generate_third_party_delivery: third_party_delivery_filename = upload_file_to_ftp('ThirdParty', suffix, third_party_deliverydata, delete_from_local_after_upload, 'aos_ftp')
if generate_primary_delivery or generate_third_party_delivery: contents_filename = upload_file_to_ftp('Contents', suffix, contents, delete_from_local_after_upload, 'aos_ftp')

# Upload data to Staq
if generate_primary_delivery and upload_data_to_staq: upload_file_to_ftp(aos_api_connection['tenantname'] + '_Primary', suffix, primary_deliverydata, delete_from_local_after_upload, 'staq_ftp_primary')
if generate_third_party_delivery and upload_data_to_staq: upload_file_to_ftp(aos_api_connection['tenantname'] + '_ThirdParty', suffix, third_party_deliverydata, delete_from_local_after_upload, 'staq_ftp_third_party')

# Trigger delivery pulls via API
if call_aos_data_pull and generate_primary_delivery: trigger_primary_delivery_pull(aos_api_connection, delivery_source_id, primary_delivery_filename)
if call_aos_data_pull and generate_third_party_delivery: trigger_third_party_delivery_pull(aos_api_connection, delivery_source_id, third_party_delivery_filename)