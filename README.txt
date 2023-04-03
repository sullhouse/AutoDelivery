Instructions:

General Config
1.  Configure autodelivery_config.py according to comments and desired usage

AOS API Access
1.  Create a user with API access to the AOS tenant and get the API key for that user
2.  Update the username, password, api key and tenantname in SAMPLE_aosapicredentials.json
3.  If using an environment other than Staging, update the authurl and apiurl
4.  Save as "aosapicredentials.json" (remove SAMPLE_ prefix) in the same location as project

Delivery Source FTP Access
1.  Update the url, folder where you want delivery data posted (or leave as "" to put data in root), username and password in SAMPLE_ftpcredentials.json
2.  Save as "ftpcredentials.json" (remove SAMPLE_ prefix) in the same location as project

Run
1.  Execute autodelivery.py