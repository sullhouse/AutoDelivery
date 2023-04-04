from ftplib import FTP
import json
import os
import pysftp
import paramiko

def upload_file_to_ftp(filename_prefix, filename_suffix, data, delete_on_local, destination):
    filename = filename_prefix + '_AutoDelivery_' + filename_suffix + '.csv'
    with open(filename, 'w') as f:
        for row in data:
            f.write("%s\n" % row)
    
    creds_file = 'ftpcredentials.json'
    with open(creds_file) as f:
        creds_json = json.load(f)
        creds = creds_json[destination]

    if creds['connectiontype'] == "ftp":
        ftp = FTP(creds['ftpurl'])
        ftp.login(creds['ftpusername'], creds['ftppassword'])
        ftp.cwd(creds['ftpfolder'])
        with open(filename, 'rb') as f:
            ftp.storbinary('STOR ' + filename, f)
        ftp.quit()

    if creds['connectiontype'] == "sftp":
        ssh_client = paramiko.SSHClient()
        # declare credentials of targeted server
        server = creds['ftpurl']
        port = 22
        user = creds['ftpusername']
        password = creds['ftppassword']
        # establish connection with targeted 
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(server,port,user,password)
        sftp = ssh_client.open_sftp()

        sftp.chdir(creds['ftpfolder'])
        sftp.put(filename, filename)

    if delete_on_local:
        os.remove(filename)
    
    return filename