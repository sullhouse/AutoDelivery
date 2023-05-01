# Set FTP space and delivery source details
delivery_source_name = 'Staq'
delivery_source_id = 'hyeKtny5Q2-buYbWfsRb6g'

# Set deals to process
external_system_ids_to_process = ['O9xQrQzFTqS1aPxRvMzVYA','028f9FSdSAKd6ZEhyrJtpQ']
unit_types_to_process = ['impressions','clicks']
date_range_start_to_process = '2023-01-01'
date_range_end_to_process = '2023-04-30'
deal_id_to_process_delivery = 0 # set to 0 to process all deals from date range above. List here will ignore date range above.

# Set delivery data to populate
previous_days = 0 # set to 0 to process for date range below
earliest_delivery_start_date = '2023-01-01' # ignored if previous days set
latest_delivery_end_date = '2023-04-26' # ignored if previous days set
generate_primary_delivery = True
generate_third_party_delivery = True
upload_delivery_data_to_staq = True
call_aos_data_pull = True
delete_from_local_after_upload = True

# Set parameters to update deal data to Staq
update_deal_data_to_staq = True
update_all_deal_data_with_delivery = False # set to True to update all deals where delivery was processed. If True the date ranges below are ignored.
deal_modified_previous_days = 5 # set to 0 to process for date range below
deal_modified_start_date = '2023-04-25' # ignored if previous days set
deal_modified_end_date = '2023-04-30' # ignored if previous days set