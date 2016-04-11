# TTFBackupService
He is a server to backup data from mysql to ftp

File list:
README.md    ------------a help document
BackupDB.c   ------------main body
BackupDB.h   
config.xml   ------------  a config file for TTFBackuper

buildcmd      ------------   how to use gcc to build him
TransportData.sh  ------------use a shell to backup data



# Latest version
V1.1.0  add logtime and  mysql lost connect mail warning

V1.1.1  add systeminfo into mail.txt

V1.2.0 update BackupDB

V1.2.1 "fail to connect to mysql" only show three times .

V1.2.2  Update BacukuDB  .

V1.3.0  after half an hour can't connect to mysql, fail.