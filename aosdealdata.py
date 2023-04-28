import datetime
import json
import pandas as pd
from aosapiconnection import *
from ftpconnection import *

deal_header_data_csv_header = 'Deal ID,Deal,Deal Order Type,Advertiser,Advertiser ID,Agency,Agency ID,Account Executive,Sales Team,Deal Start Period,Deal End Period,Deal Start Date,Deal End Date,Sales Stage,Currency,Ratecard,Deal Status,Deal Last Modified Date,Deal Last Modified By,Deal Created By,Deal Created Date'
deal_lineitem_data_csv_header = 'Line Item ID,Line Item Name,Third Party Billable Ad Server,Cost Method,Unit Type,Sold Quantity,SOV,Production Quantity,Production Buffer,Net Unit Cost,Net Cost,Gross Unit Cost,Gross Cost,Gross Discount,Total Discount,Unit Cost Before Discounts,Unit Cost Before Discounts With Premiums,Line Item Value,Suggested Buffer,Rate Card Price,Goal Price,Floor Price,UCBD Below Goal,Net Unit Cost Below Goal,Net Unit Cost Below Floor,Line Item Start Date,Line Item Start Time,Line Item End Date,Line Item End Time,timeZone,Product,Line Item Deal Line Number,Can Export,Can Produce,Has Performance,Can Invoice,ADU,Bonus,Makegood,Recap,Parent Line Item ID,Line Item Type,Package Type,Order Status,Finance Status,Line Item Last Modified Date,Line Item Last Modified By,Line Item Created Date,Line Item Created By'

def get_deal_data(aos_api_connection,deal_id):
    deal_data = {}
    deal_data['deal_header'] = get_deal_header_from_aos_api(aos_api_connection,deal_id)
    deal_data['deal_lineitems'] = get_deal_lineitems_from_aos_api(aos_api_connection,deal_id)
    return deal_data

def get_deal_header_csv_row(deal_header):
    deal_data_csv_row = (
        str(deal_header['planId']) + ',' +
        strForCSV(deal_header['planName']) + ',' +
        strForCSV(deal_header['orderType']['name']) + ','
    )
    
    deal_data_csv_row += ('"' + strForCSV(deal_header['advertisers'][0]['name']) + '",')
    deal_data_csv_row += (deal_header['advertisers'][0]['id'] + ',')
    
    try:
        deal_data_csv_row += (strForCSV(deal_header['agencies'][0]['name']) + ',')
    except IndexError:
        deal_data_csv_row += ','
    except TypeError:
        deal_data_csv_row += ','

    try:
        deal_data_csv_row += deal_header['agencies'][0]['mdmId'] + ','
    except IndexError:
        deal_data_csv_row += ','
    except TypeError:
        deal_data_csv_row += ','

    deal_data_csv_row += (
        strForCSV(deal_header['accountExecutives'][0]['name']) + ','
    )

    try:
        deal_data_csv_row += (strForCSV(deal_header['accountExecutives'][0]['team']['name']) + ',')
    except KeyError:
        deal_data_csv_row += ','
    except TypeError:
        deal_data_csv_row += ','

    deal_data_csv_row += (
        str(deal_header['startPeriod']['name']) + ',' +
        str(deal_header['endPeriod']['name']) + ',' +
        deal_header['startDate'] + ',' +
        deal_header['endDate'] + ','
    )

    try:
        deal_data_csv_row += (deal_header['salesStage']['name'] + ',')
    except TypeError:
        deal_data_csv_row += ','

    try:
        deal_data_csv_row += (deal_header['currency']['name'] + ',')
    except TypeError:
        deal_data_csv_row += ','
    
    try:
        deal_data_csv_row += (deal_header['digitalRatecards'][0]['name'] + ',')
    except IndexError:
        deal_data_csv_row += ','
    except TypeError:
        deal_data_csv_row += ','

    deal_data_csv_row += (
        str(deal_header['planStatus']['statusName']) + ',' +
        '"' + deal_header['audit']['lastModifiedDate'] + '",' +
        deal_header['audit']['lastModifiedBy'] + ',' +
        deal_header['audit']['createdBy'] + ',' +
        '"' + deal_header['audit']['createdDate'] + '"'
    )

    return deal_data_csv_row

def get_deal_lineitems_csv_row(lineitem, deal_helper_data):
    if lineitem['thirdPartyBillableAdServerId'] is not None:
        thirdparty_billable_ad_server = str(next(item for item in deal_helper_data['external_systems'] if item['id'] == lineitem['thirdPartyBillableAdServerId'])['name'])
    else:
        thirdparty_billable_ad_server = 'None'

    if lineitem['packageLine']:
        product_name = str(next(item for item in deal_helper_data['packages']['productGroups'] if item['productLineId'] == lineitem['planWorkspaceProduct']['productId'])['productLineName'])
        parent_line_id = "None"
    else:
        product_name = str(next(item for item in deal_helper_data['products']['response'] if item['productID'] == lineitem['planWorkspaceProduct']['productId'])['productName'])
        if lineitem['standardLine']:
            parent_line_id = "None"
        else:
            parent_line_id = str(next(item for item in deal_helper_data['group_lineitem_ids'] if item['id'] == lineitem['planWorkspaceProduct']['packageLineId'])['sequenceId'])
    
    try:
        cost_method_name = str(next(item for item in deal_helper_data['cost_methods'] if item['id'] == lineitem['rates']['costMethodId'])['name'])
    except StopIteration:
        cost_method_name = "n/a"
    except TypeError:
        cost_method_name = "n/a"
    
    try:
        unit_type_name = str(next(item for item in deal_helper_data['unit_types'] if item['id'] == lineitem['rates']['unitTypeId'])['name'])
    except StopIteration:
        unit_type_name = "n/a"
    except TypeError:
        unit_type_name = "n/a"

    lineintem_csv_row = ("" +
        str(lineitem['sequenceId']) + ',' +
        strForCSV(lineitem["name"]) + ',' +
        thirdparty_billable_ad_server + ',' +
        cost_method_name + ',' +
        unit_type_name + ',' +
        str(lineitem['rates']['quantity']) + ',' +
        str(lineitem['rates']['sov']) + ',' +
        str(lineitem['rates']['productionQuantity']) + ',' +
        str(lineitem['rates']['productionBuffer']) + ',' +
        str(lineitem['rates']['netUnitCost']) + ',' +
        str(lineitem['rates']['netCost']) + ',' +
        str(lineitem['rates']['grossUnitCost']) + ',' +
        str(lineitem['rates']['grossCost']) + ',' +
        str(lineitem['rates']['grossDiscount']) + ',' +
        str(lineitem['rates']['totalDiscount']) + ',' +
        str(lineitem['rates']['ucbd']) + ',' +
        str(lineitem['rates']['ucbdwp']) + ',' +
        str(lineitem['rates']['lineValue']) + ',' +
        str(lineitem['rates']['suggestedBuffer']) + ',' +
        str(lineitem['rates']['rcPrices']['ratecardPrice']) + ',' +
        str(lineitem['rates']['rcPrices']['goalPrice']) + ',' +
        str(lineitem['rates']['rcPrices']['floorPrice']) + ',' +
        str(lineitem['rates']['ucbdBelowGoalPrice']) + ',' +
        str(lineitem['rates']['netUnitCostBelowGoalPrice']) + ',' +
        str(lineitem['rates']['netUnitCostBelowFloorPrice']) + ',' +
        str(lineitem['period']['startDate']) + ',' +
        str(lineitem['period']['startTime']) + ',' +
        str(lineitem['period']['endDate']) + ',' +
        str(lineitem['period']['endTime']) + ',' +
        str(lineitem['period']['timeZone']) + ',' +
        product_name + ',' +
        str(lineitem['planWorkspaceProduct']['lineNo']) + ',' +
        str(lineitem['planWorkspaceProduct']['lineClassAttributes']['canExport']) + ',' +
        str(lineitem['planWorkspaceProduct']['lineClassAttributes']['canProduce']) + ',' +
        str(lineitem['planWorkspaceProduct']['lineClassAttributes']['hasPerformance']) + ',' +
        str(lineitem['planWorkspaceProduct']['lineClassAttributes']['canInvoice']) + ',' +
        str(lineitem['planWorkspaceProduct']['lineClassAttributes']['adu']) + ',' +
        str(lineitem['planWorkspaceProduct']['lineClassAttributes']['bonus']) + ',' +
        str(lineitem['planWorkspaceProduct']['lineClassAttributes']['makegood']) + ',' +
        str(lineitem['planWorkspaceProduct']['lineClassAttributes']['recap']) + ',' +
        parent_line_id + ',' +
        str(lineitem['planWorkspaceProduct']['lineType']) + ',' +
        str(lineitem['planWorkspaceProduct']['packageType']) + ',' +
        str(lineitem['orderStatus']['salesLineStatus']) + ',' +
        str(lineitem['financeStatus']['financeLineStatus']) + ',' +
        '"' + lineitem['lastModifiedDate'] + '",' +
        lineitem['lastModifiedBy'] + ',' +
        '"' + lineitem['createdDate'] + '",' +
        lineitem['createdBy']
    )

    return lineintem_csv_row

def get_deal_data_csv_rows(aos_api_connection, deal_id):
    deal_data = get_deal_data(aos_api_connection,deal_id)
    deal_header_csv = get_deal_header_csv_row(deal_data['deal_header'])
    deal_data_csv = []
    deal_helper_data = get_deal_helper_data(aos_api_connection, deal_data)
    if deal_data['deal_lineitems']:
        for lineitem in deal_data['deal_lineitems']:
            deal_data_csv_row = deal_header_csv + ',' + get_deal_lineitems_csv_row(lineitem, deal_helper_data)
            deal_data_csv.append(deal_data_csv_row)
    else:
        deal_data_csv_row = deal_header_csv + ',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,'
        deal_data_csv.append(deal_data_csv_row)

    return deal_data_csv

def get_deal_data_csv_header():
    return deal_header_data_csv_header + ',' + deal_lineitem_data_csv_header

def get_deal_data_csv_all(aos_api_connection, deal_ids):
    deal_data = []
    deal_data.append(get_deal_data_csv_header())
    print(deal_data[-1])
    for deal_id in deal_ids:
        deal_data.extend(get_deal_data_csv_rows(aos_api_connection, deal_id))
        print(deal_data[-1])

    return deal_data

def get_deal_helper_data(aos_api_connection, deal_data):
    deal_helper_data = {}
    deal_helper_data['external_systems'] = get_external_systems_from_aos_api(aos_api_connection)
    deal_helper_data['cost_methods'] = get_cost_methods_from_aos_api(aos_api_connection)
    deal_helper_data['unit_types'] = get_unit_types_from_aos_api(aos_api_connection)
    deal_helper_data['products'] = get_products_from_aos_api(aos_api_connection)
    deal_helper_data['packages'] = get_packages_from_aos_api(aos_api_connection)
    deal_helper_data['group_lineitem_ids'] = []
    for lineitem in deal_data['deal_lineitems']: 
        if lineitem['packageLine']: deal_helper_data['group_lineitem_ids'].append({"sequenceId": lineitem['sequenceId'], "id": lineitem['id']})
    
    return deal_helper_data

def strForCSV(s):
    s = s.replace("\"", "\\\"")
    s = f'"{s}"'
    return s