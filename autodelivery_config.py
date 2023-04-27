# Set FTP space and delivery source details
delivery_source_name = 'Staq'
delivery_source_id = 'hyeKtny5Q2-buYbWfsRb6g'

# Set deals to process
external_system_ids_to_process = ['O9xQrQzFTqS1aPxRvMzVYA','028f9FSdSAKd6ZEhyrJtpQ']
unit_types_to_process = ['impressions','clicks']
date_range_start_to_process = '2023-01-01'
date_range_end_to_process = '2023-05-31'
deal_id_to_process = 564 # set to 0 to process all deals

# Set delivery data to populate
previous_days = 2 # set to 0 to process for date range below
earliest_delivery_start_date = '2023-03-01' # ignored if previous days set
latest_delivery_end_date = '2023-04-04' # ignored if previous days set
generate_primary_delivery = True
generate_third_party_delivery = True
upload_data_to_staq = True # this includes the delivery data as well as the latest data about all deals processed
call_aos_data_pull = True
delete_from_local_after_upload = True