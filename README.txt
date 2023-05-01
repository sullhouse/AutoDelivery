AutoDelivery is a script that is meant to be run either ad hoc or on a scheduled basis to create random but realistic performance data in AOS and Staq for existing fake Deals, Orders and Invoices. Today it is configurable to run against a specified set of deals, or all deals, for specified external systems and unit types, and for business in a specified date range. The delivery data to generate can be either a previous set of days (useful if the job is scheduled, every day generate new data for the previous day) or a date range (useful to populate all delivery for an entire deal).

General Config
1.  Configure SAMPLE_autodelivery_config.py according to comments and desired usage and save as "autodelivery_config.py" (remove SAMPLE_ prefix)

AOS API Access
1.  Create a user with API access to the AOS tenant and get the API key for that user
2.  Update the username, password, api key and tenantname in SAMPLE_aosapicredentials.json
3.  If using an environment other than Staging, update the authurl and apiurl
4.  Save as "aosapicredentials.json" (remove SAMPLE_ prefix) in the same location as project

FTP Access
1.  For AOS Delivery Source, update the url, folder, username and password in SAMPLE_ftpcredentials.json in aos_ftp
2.  For Staq upload, update the url, folder, username and password in SAMPLE_ftpcredentials.json in staq_ftp
3.  Save as "ftpcredentials.json" (remove SAMPLE_ prefix) in the same location as project

Run
1.  Execute autodelivery.py
