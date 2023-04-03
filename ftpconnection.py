from ftplib import FTP
import json
import os

def upload_file_to_ftp(filename_prefix, filename_suffix, data, delete_on_local):
    filename = filename_prefix + '_AutoDelivery_' + filename_suffix + '.csv'
    with open(filename, 'w') as f:
        for row in data:
            f.write("%s\n" % row)
    
    creds_file = 'ftpcredentials.json'
    with open(creds_file) as f:
        creds = json.load(f)
    ftp = FTP(creds['ftpurl'])
    ftp.login(creds['ftpusername'], creds['ftppassword'])
    ftp.cwd(creds['ftpfolder'])
    with open(filename, 'rb') as f:
        ftp.storbinary('STOR ' + filename, f)
    ftp.quit()

    if delete_on_local:
        os.remove(filename)
    
    return filename