import datetime
import random

# Get data needed from a lineitem to create a delivery line item
def get_deliverylineitem(lineitem, unit_types_to_process, workstream, generate_third_party_delivery):
    if 'everPushCompleted' in lineitem and lineitem['everPushCompleted'] == True and lineitem['unitType']['name'].lower() in unit_types_to_process and lineitem['pushQuantity'] > 10:
                
        # Get line item details needed for the delivery file
        deliverylineitem = {
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
            'associated_sales_lineitem_id': lineitem['dealLineSequenceId']
        }
        if generate_third_party_delivery and 'thirdPartyBillableAdServer' in lineitem:
            deliverylineitem['third_party_system_id'] = lineitem['thirdPartyBillableAdServer']['id']
            deliverylineitem['third_party_system_name'] = lineitem['thirdPartyBillableAdServer']['name']
    
        return deliverylineitem
    
    return None

# Get the dates and quantities to use in delivery files
def get_dates_quantities(deliverylineitem, earliest_delivery_start_date, latest_delivery_end_date):
    dates_quantities = []
    line_start_date = datetime.datetime.strptime(deliverylineitem['start_date'], "%Y-%m-%d").date()
    delivery_start_date = max(earliest_delivery_start_date, line_start_date)
    line_end_date = datetime.datetime.strptime(deliverylineitem['end_date'], "%Y-%m-%d").date()
    delivery_end_date = min(latest_delivery_end_date, line_end_date)
    delta = datetime.timedelta(days=1)
    days_in_line_item = (line_end_date - line_start_date).days + 1
    quantity_per_day = deliverylineitem['quantity'] / days_in_line_item
    delivery_date = delivery_start_date
    while delivery_date <= delivery_end_date:
        quantity=quantity=round(quantity_per_day * (1 + random.uniform(-0.05, 0.05)))
        date_quantity = {
            "delivery_date": delivery_date,
            "primary_quantity": quantity,
            "third_party_quantity": round(quantity * (1 + random.uniform(-0.05, 0.0)))
        }

        dates_quantities.append(date_quantity)
        delivery_date += delta
    
    return dates_quantities

# Define a primary delivery csv header and how to generate each row of data
primary_delivery_csv_header = 'Production System ID,Production System Name,Delivered Date,Advertiser ID,Advertiser Name,Order ID,Order Name,Line Item ID,Line Item Name,Unit Type 1,Delivered Quantity 1,Unit Type 2,Delivered Quantity 2,Start Date,End Date,Unit Cost,Cost Method,Associated Sales Line Item ID'

def get_primary_delivery_csv_row(deliverylineitem, delivery_date, quantity):
    return (
        deliverylineitem['external_system_id'] + ',' +
        deliverylineitem['external_system_name'] + ',' +
        str(delivery_date) + ',' +
        deliverylineitem['external_advertiser_id'] + ',' +
        deliverylineitem['external_advertiser_name'] + ',' +
        deliverylineitem['external_order_id'] + ',' +
        deliverylineitem['external_order_name'] + ',' +
        deliverylineitem['external_line_id'] + ',' +
        deliverylineitem['external_line_name'] + ',' +
        deliverylineitem['unit_type'] + ',' +
        str(quantity) + ',' +
        'clicks,' + str(round(quantity * .03)) + ',' +
        deliverylineitem['start_date'] + ',' +
        deliverylineitem['end_date'] + ',' +        
        str(deliverylineitem['unit_cost']) + ',' +
        deliverylineitem['cost_method'] + ',' +
        deliverylineitem['associated_sales_lineitem_id']
    )

# Define a third party delivery csv header and how to generate each row of data
third_party_delivery_csv_header = 'Production System ID,Production System Name,Delivered Date,Advertiser ID,Advertiser Name,Order ID,Order Name,Line Item ID,Line Item Name,Unit Type 1,Delivered Quantity 1,Unit Type 2,Delivered Quantity 2,Start Date,End Date,Associated Production System ID,Associated Production System Name,Associated Line Item ID,Associated Line Item Name'

def get_third_party_delivery_csv_row(deliverylineitem, delivery_date, quantity):
    return (
        deliverylineitem['third_party_system_id'] + ',' +
        deliverylineitem['third_party_system_name'] + ',' +
        str(delivery_date) + ',' +
        deliverylineitem['external_advertiser_id'] + '_tp,' +
        deliverylineitem['external_advertiser_name'] + ',' +
        deliverylineitem['external_order_id'] + '_tp,' +
        deliverylineitem['external_order_name'] + ',' +
        deliverylineitem['external_line_id'] + '_tp,' +
        deliverylineitem['external_line_name'] + '_' + deliverylineitem['third_party_system_name'] + ',' +
        deliverylineitem['unit_type'] + ',' +
        str(quantity) + ',' +
        'clicks,' + str(round(quantity * .03)) + ',' +
        deliverylineitem['start_date'] + ',' +
        deliverylineitem['end_date'] + ',' +
        deliverylineitem['external_system_id'] + ',' +
        deliverylineitem['external_system_name'] + ',' +
        deliverylineitem['external_line_id'] + ',' +
        deliverylineitem['external_line_name']
    )

# Define the contents csv header and how to generate each row of data
contents_csv_header = 'Primary Production System ID,Primary Production System Name,Advertiser ID,Advertiser Name,Order ID,Order Name,Start Date,End Date,Primary Line Item Count,Third Party Line Item Count'

def get_contents_csv_row(deliverylineitem, primary_line_item_count, third_party_line_item_count):
    return (
        deliverylineitem['external_system_id'] + ',' +
        deliverylineitem['external_system_name'] + ',' +
        deliverylineitem['external_advertiser_id'] + ',' +
        deliverylineitem['external_advertiser_name'] + ',' +
        deliverylineitem['external_order_id'] + ',' +
        deliverylineitem['external_order_name'] + ',' +
        deliverylineitem['start_date'] + ',' +
        deliverylineitem['end_date'] + ',' +        
        str(primary_line_item_count) + ',' +
        str(third_party_line_item_count)
    )