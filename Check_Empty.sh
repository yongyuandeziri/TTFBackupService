#!/bin/bash
MainPath="/home/asusftp/"
DataBaseDir=""

DirName=$(date +"%Y%m%d")
find "$MainPath$DataBaseDir" -type d -empty | grep $DirName
if [ "`echo $?`" = "0" ];then
	echo "Has empty dir $DirName,try to sleep 30s"
	sleep 30
	find "$MainPath$DataBaseDir" -type d -empty | grep $DirName
	if [ "`echo $?`" = "0" ];then
	echo "$DirName" >> "$MainPath/.empty"
	else
	echo "Dir $DirName is not empty now"
	fi 

else
	echo "Dir $DirName is not empty,continue"
fi
