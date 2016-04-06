#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <libxml/xmlmemory.h>
#include <libxml/parser.h>
#include "BackupDB.h"

#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>
#include <ctype.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/user.h>
#include <linux/types.h>
#include <linux/fs.h>

#include <mysql/mysql.h>

MYSQL *g_conn;//mysql connection
MYSQL_RES *g_res;
MYSQL_ROW g_row;

#define MAX_BUF_SIZE 1024
#define MAX_TABLE_SIZE 512
#define MAX_DATABASE_SIZE 32
typedef int bool;/*ward necessary?*/
#define ture 1
#define false 0

typedef struct 
{
    int maxid;
    int psid;
    xmlChar tablename[MAX_BUF_SIZE];
    bool needbackup;
} Table;

typedef struct 
{
    xmlChar * host;
    xmlChar * port;
    xmlChar * username;
    xmlChar * password;
    xmlChar * project;
    xmlChar * database;
    xmlChar * idpath;
    Table tables[MAX_TABLE_SIZE];
    int dbtable_num;
    xmlChar Idbackpath[50];
} DataBase;

DataBase DataBaseInfo[10];
xmlChar * backpath="";
xmlChar * logpath="";
int i=0;int interval=0;int maxrows=0;int hdflag=0;
xmlChar* EmailFromAddr="";
xmlChar* EmailFromName="";
xmlChar* EmailFromPassword="";
xmlChar* EmailToAddr="";
xmlChar* EmailTitle="";
xmlChar* EmailBodyPath="";
xmlChar* EmailConfig="";
xmlChar* EmailServer="";
xmlChar SendEmailCMD[512]={0};
xmlChar* ServerInfo="";
FILE *logfile;
int logfile_num;
int isempty=0;

#ifndef Debug
#define myprintf(fmt,arg...) fprintf(logfile,fmt,##arg)
#else
#define myprintf(fmt,arg...) printf(fmt,##arg)
#endif


void dechar(char *str,int len)//remove space,tab,enter
{  
    char tmp[1024];  
    int t=0; int i;
    for(i=0;i<len;i++)  
    if(!(str[i]==' ' || str[i]=='\t' || str[i]=='\n')) tmp[t++]=str[i];  
    tmp[t]='\0';  
    strcpy(str,tmp);  
}  

void print_mysql_error(const char *msg) 
{
    if (msg)
        myprintf("%s: %s\n", msg, mysql_error(g_conn));
     else
        myprintf("%s\n",mysql_error(g_conn));
 }
 
int executesql(const char * sql)
{
    /*query the database according the sql*/
    if (mysql_real_query(g_conn, sql, strlen(sql)))
        return -1;//fail
 
    return 0; //success
}

void Get_Sendemail_CMD()
{
    strcat(SendEmailCMD,"sendemail -s ");
    strcat(SendEmailCMD,EmailServer);
    strcat(SendEmailCMD," -f ");
    strcat(SendEmailCMD,EmailFromAddr);
    strcat(SendEmailCMD," -t ");
    strcat(SendEmailCMD,EmailToAddr);
    strcat(SendEmailCMD," -u ");
    strcat(SendEmailCMD,EmailTitle);
    strcat(SendEmailCMD," -xu ");
    strcat(SendEmailCMD,EmailFromName);
    strcat(SendEmailCMD," -xp ");
    strcat(SendEmailCMD,EmailFromPassword);
    strcat(SendEmailCMD," -o ");
    strcat(SendEmailCMD,EmailConfig);
    strcat(SendEmailCMD," -o message-file=");
    strcat(SendEmailCMD,EmailBodyPath);
    strcat(SendEmailCMD," &");
    myprintf("%s\n", SendEmailCMD);
}

int parseStory(xmlDocPtr doc, xmlNodePtr cur)
{
    xmlChar *key;cur = cur->xmlChildrenNode;
    xmlNodePtr MysqlInfo;xmlNodePtr MysqlProjectInfo;
    while(cur != NULL)
    {
        if((!xmlStrcmp(cur->name, (const xmlChar*)"BackPath")))
        {
            backpath=xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            myprintf("%s=>%s\n", cur->name, backpath);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"LogPath")))
        {
            logpath=xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            myprintf("%s=>%s\n", cur->name, logpath);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"Interval")))
        {
            interval=atoi(xmlNodeListGetString(doc, cur->xmlChildrenNode, 1));
            myprintf("%s=>%d\n", cur->name, interval);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"MaxRows")))
        {
            maxrows=atoi(xmlNodeListGetString(doc, cur->xmlChildrenNode, 1));
            myprintf("%s=>%d\n", cur->name, maxrows);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"EmailServer")))
        {
            EmailServer=xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            myprintf("%s=>%s\n", cur->name, EmailServer);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"EmailFromAddr")))
        {
            EmailFromAddr=xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            myprintf("%s=>%s\n", cur->name, EmailFromAddr);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"EmailFromName")))
        {
            EmailFromName=xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            myprintf("%s=>%s\n", cur->name, EmailFromName);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"EmailFromPassword")))
        {
            EmailFromPassword=xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            myprintf("%s=>%s\n", cur->name, EmailFromPassword);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"EmailToAddr")))
        {
            EmailToAddr=xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            myprintf("%s=>%s\n", cur->name, EmailToAddr);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"EmailTitle")))
        {
            EmailTitle=xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            myprintf("%s=>%s\n", cur->name, EmailTitle);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"EmailBodyPath")))
        {
            EmailBodyPath=xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            myprintf("%s=>%s\n", cur->name, EmailBodyPath);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"EmailConfig")))
        {
            EmailConfig=xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            myprintf("%s=>%s\n", cur->name, EmailConfig);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"ServerInfo")))
        {
            ServerInfo=xmlNodeListGetString(doc, cur->xmlChildrenNode, 1);
            myprintf("%s=>%s\n", cur->name, ServerInfo);
        }
        else if((!xmlStrcmp(cur->name, (const xmlChar*)"MySQL")))
        {
            xmlChar * host="";xmlChar * port="";xmlChar * username="";
            xmlChar * password="";xmlChar * project="";xmlChar * database="";
            MysqlInfo=cur->xmlChildrenNode;
            while(MysqlInfo!=NULL)
            {
                if((!xmlStrcmp(MysqlInfo->name, (const xmlChar*)"Name")))
                {
                    key=xmlNodeListGetString(doc, MysqlInfo->xmlChildrenNode, 1);
                    myprintf("%s=>%s\n", MysqlInfo->name, key);
                    // xmlFree(key);
                }
                else if((!xmlStrcmp(MysqlInfo->name, (const xmlChar*)"Server")))
                {
                    host=xmlNodeListGetString(doc, MysqlInfo->xmlChildrenNode, 1);
                    // myprintf(" %s=>%s\n", MysqlInfo->name, host);
                }
                else if((!xmlStrcmp(MysqlInfo->name, (const xmlChar*)"Port")))
                {
                    port=xmlNodeListGetString(doc, MysqlInfo->xmlChildrenNode, 1);
                    // myprintf(" %s=>%s\n", MysqlInfo->name, port);
                }
                else if((!xmlStrcmp(MysqlInfo->name, (const xmlChar*)"User")))
                {
                    username=xmlNodeListGetString(doc, MysqlInfo->xmlChildrenNode, 1);
                    // myprintf(" %s=>%s\n", MysqlInfo->name, username);
                }
                else if((!xmlStrcmp(MysqlInfo->name, (const xmlChar*)"Password")))
                {
                    password=xmlNodeListGetString(doc, MysqlInfo->xmlChildrenNode, 1);
                    // myprintf(" %s=>%s\n", MysqlInfo->name, password);
                }
                else if((!xmlStrcmp(MysqlInfo->name, (const xmlChar*)"Project")))
                {
                    MysqlProjectInfo=MysqlInfo->xmlChildrenNode;
                    while(MysqlProjectInfo!=NULL)
                    {
                        if((!xmlStrcmp(MysqlProjectInfo->name, (const xmlChar*)"Name"))){
                        project=xmlNodeListGetString(doc, MysqlProjectInfo->xmlChildrenNode, 1);
                        // myprintf(" %s=>%s\n", MysqlProjectInfo->name, project);
                        }
                        else if((!xmlStrcmp(MysqlProjectInfo->name, (const xmlChar*)"Database")))
                        {
                            database=xmlNodeListGetString(doc, MysqlProjectInfo->xmlChildrenNode, 1);
                            // myprintf(" %s=>%s\n", MysqlProjectInfo->name, database);
                        }

                        MysqlProjectInfo=MysqlProjectInfo->next;
                    }

                    DataBaseInfo[i].host=host;
                    DataBaseInfo[i].port=port;
                    DataBaseInfo[i].username=username;
                    DataBaseInfo[i].password=password;
                    DataBaseInfo[i].project=project;
                    DataBaseInfo[i].database=database;
                    i=i+1;

                }     

                MysqlInfo=MysqlInfo->next;

            }
        }
        cur = cur->next;
    }
    
    return 0;
}

char* Exec_Shell_Cmd(char* cmd)
{
    FILE* stream;
    char buf[1024];
    char *RetVal=NULL;

    memset(buf,0,sizeof(buf));
    stream = popen(cmd,"r");

    if(stream == NULL)
    {
        myprintf("popen open file error!\n");
        return NULL;
    }
    else
    {
        if(fgets(buf,sizeof(buf),stream) != NULL)
        {
            RetVal=buf;
        }
        pclose(stream);
    }

    return RetVal;
}

int check_file_exist(char* dbpsidpath)
{
    FILE *fp=NULL;
    if((fp=fopen(dbpsidpath,"r"))==NULL)
    {
        return -1;
    }
    else
    {
        fclose(fp);
        return 0;
    }
}

char* get_data_dir()
{
    return Exec_Shell_Cmd("date +%Y%m%d");
}

char* get_time_filename()
{
    return Exec_Shell_Cmd("date +%H%M%S");
}

int check_hddisk_remain()
{
    xmlChar temp_cmd[512]={0}; xmlChar GetValue[8]={0};
    strcpy(temp_cmd,"df ");
    strcat(temp_cmd,backpath);
    strcat(temp_cmd," | grep -A1 -v \"Use%\" | awk '{print $5}'");
    strcpy(GetValue,Exec_Shell_Cmd(temp_cmd));
    return atoi(GetValue);
}


int check_process_is_running()
{
    return atoi(Exec_Shell_Cmd("ps -ef | grep BackupDB | grep config.xml |wc -l"));
}

bool backup_table_data(char* host,char* user,char* pass,char* database,char* table,char* start,char* end,char* filename,char*dirname)
{
    //mysqldump --no-create-info --skip-lock-tables -q -h10.64.58.199 -uASUSTDC -pasustdc ttfdb ttfis --where "id>=4201 and id<=4300" > /home/asusftp/ttfdb/ttfis/20160225/151903_4201-4300.sql
    xmlChar cmd[1024]={0};xmlChar mkdir_cmd[512]={0};xmlChar temp_filename[1024]={0};
    xmlChar rename_cmd[1024]={"mv "};
    
    //mkdir by date
    strcat(mkdir_cmd,"mkdir -p ");//if exist,donothing; else midir by days
    strcat(mkdir_cmd,dirname);
    Exec_Shell_Cmd(mkdir_cmd);

    strcat(cmd,"mysqldump --no-create-info --skip-lock-tables -q -h");
    strcat(cmd,host);
    strcat(cmd," -u");
    strcat(cmd,user);
    strcat(cmd," -p");
    strcat(cmd,pass);
    strcat(cmd," ");
    strcat(cmd,database);
    strcat(cmd," ");
    strcat(cmd,table);
    strcat(cmd," --where \"id>=");
    strcat(cmd,start);
    strcat(cmd," and id<=");
    strcat(cmd,end);
    strcat(cmd,"\"");
    strcat(cmd," > ");
    strcpy(temp_filename,filename);
    strcat(temp_filename,".temp");
    strcat(cmd,temp_filename);
    Exec_Shell_Cmd(cmd);

    //rename file,take off .temp
    strcat(rename_cmd,temp_filename);
    strcat(rename_cmd," ");
    strcat(rename_cmd,filename);
    Exec_Shell_Cmd(rename_cmd);
    // myprintf("rename temp_filename to filename %s\n",rename_cmd);
}

void create_file_name(char* filename,char* dirname,char* database,char* table,char *start,char* end,char*backpath)
{
    char temp[1024]={0};char temp_file[1024]={0};
    strcat(filename,backpath);
    strcat(filename,database);
    strcat(filename,"/");
    strcat(filename,table);
    strcat(filename,"/");
    strcpy(temp,get_data_dir());//dir name by days
    dechar(temp,sizeof(temp));
    strcat(filename,temp);
    strcpy(dirname,filename);

    strcat(temp_file,"/");
    strcat(temp_file,get_time_filename());//file name by time
    dechar(temp_file,sizeof(temp_file));
    strcat(filename,temp_file);
    strcat(filename,"_");
    strcat(filename,start);
    strcat(filename,"-");
    strcat(filename,end);
    strcat(filename,".sql");
}

long rotate_log(xmlChar* logfilepath)
{
    if(check_file_exist(logfilepath)==-1) 
    {
        myprintf("can't find %s\n",logfilepath);
        fsync(logfile_num);
        fclose(logfile);
        exit(0);
    }

    long filesize = -1;      
    struct stat statbuff;  
    if(stat(logfilepath, &statbuff) < 0){  
        return filesize;  
    }else{  
        filesize = statbuff.st_size;  
    }  
    
    // if size is too large ,rename it
    if(filesize >= 4194304)
     {
        xmlChar logname[1024]={0};xmlChar datename[512]={0};xmlChar timename[512]={0};
        fsync(logfile_num);
        fclose(logfile);//close fd

        strcpy(logname,"mv ");
        strcat(logname,logfilepath);
        strcat(logname," ");

        strcpy(datename,logpath);
        strcat(datename,get_data_dir());
        dechar(datename,sizeof(logname));
        strcat(datename,"_");

        strcat(timename,get_time_filename());
        dechar(timename,sizeof(logname));

        strcat(logname,datename);
        strcat(logname,timename);
        strcat(logname,".txt");
        myprintf("logname %s\n",logname);//mv /home/asusftp/log/.log.txt /home/asusftp/log/20160229_193708.txt
        Exec_Shell_Cmd(logname);//rename .log.txt

        logfile=fopen("/home/asusftp/log/.log.txt","a");//new .log.txt
     } 
}

int main(int argc, char **argv)
{
    int j,k,num;char *RetVal;char sql[MAX_BUF_SIZE];
    xmlDocPtr doc;xmlNodePtr cur;
    int t=0;
    //open /home/asusftp/log/.log.txt,this is for BackupDB log
    logfile=fopen("/home/asusftp/log/.log.txt","a");
    logfile_num=fileno(logfile);

    //check if other same process is running(grep also should add 1)
    myprintf("check_process_is_running() %d\n", check_process_is_running());
    if(check_process_is_running()>=3) 
    {
        myprintf("other same process is already running\n");
        fsync(logfile_num);
        fclose(logfile);
        exit(0);
    }

    //change type to utf8
    doc=xmlParseFile(argv[1]);
    if(doc==NULL)
    {
        myprintf("config.xml can't be parsed successfully.\n");
        fsync(logfile_num);
        fclose(logfile);
        exit(1);
    }

    //get root node
    cur=xmlDocGetRootElement(doc);
    if(cur==NULL)
    {
        myprintf("config.xml is empty.\n");
        xmlFreeDoc(doc);
        fsync(logfile_num);
        fclose(logfile);
        exit(1);
    }

    //get root pointer,here is config
    if (xmlStrcmp(cur->name, (const xmlChar *)"Config"))
    {
        myprintf("config.xml has the wrong type,root node!=Config\n");
        xmlFreeDoc(doc);
        fsync(logfile_num);
        fclose(logfile);
        exit(1);
    }

    //parse config.xml
    parseStory(doc, cur);
    xmlFreeDoc(doc);

    //get sendemail cmd
    Get_Sendemail_CMD();

    //init db idfile path
    for(j=0;j<i;j++)//scan every database,i is the total database number
    {
        //strcat string
        strcpy(DataBaseInfo[j].Idbackpath,backpath);
        strcat(DataBaseInfo[j].Idbackpath,".");
        strcat(DataBaseInfo[j].Idbackpath,DataBaseInfo[j].database);
        myprintf("DataBaseInfo.Idbackpath %s\n",DataBaseInfo[j].Idbackpath);
        myprintf("DataBaseInfo[%d].host %s\n",j,DataBaseInfo[j].host);
        myprintf("DataBaseInfo[%d].port  %s\n",j,DataBaseInfo[j].port);
        myprintf("DataBaseInfo[%d].username %s\n",j,DataBaseInfo[j].username);
        myprintf("DataBaseInfo[%d].password %s\n",j,DataBaseInfo[j].password);
        myprintf("DataBaseInfo[%d].project %s\n",j,DataBaseInfo[j].project);
        myprintf("DataBaseInfo[%d].database %s\n",j,DataBaseInfo[j].database);
    }

while(1)
{
    myprintf("logtime:%s\n", Exec_Shell_Cmd("date +%Y%m%d_%H%M%S"));
    for(j=0;j<i;j++)//i=total database num
    {        
        /*********Init database connection*********/    
        g_conn = mysql_init(NULL);
        if(!mysql_real_connect(g_conn, DataBaseInfo[j].host, DataBaseInfo[j].username, DataBaseInfo[j].password, DataBaseInfo[j].database, atoi(DataBaseInfo[j].port), NULL, 0)) // 如果失败
        {
	    if(t<3)
	    {
		  //connect to mysql fail,try to send email to asus
	    	xmlChar connect_mysql[512]={0};
		strcpy(connect_mysql,"echo ");
		strcat(connect_mysql,ServerInfo);
		strcat(connect_mysql,"connect mysql fail");
		strcat(connect_mysql," ");
		strcat(connect_mysql," >>");
		strcat(connect_mysql,EmailBodyPath);
		myprintf("server info %s\n",connect_mysql);
		Exec_Shell_Cmd(connect_mysql);
		usleep(100*1000);
	    	myprintf("Fail to connect db %s...\n",DataBaseInfo[j].database);
		printf("send email %s\n",SendEmailCMD);
		Exec_Shell_Cmd(SendEmailCMD);	
		t++;
	    }
            continue;
        }
        else
        {
        /*********get db's tables *********/
             sprintf(sql,"show tables");
             if(executesql(sql))
             {
                print_mysql_error(NULL);
             }
             g_res=mysql_store_result(g_conn);
             int iNum_rows=mysql_num_rows(g_res);
             myprintf("------------%s------------ %d tables\n",DataBaseInfo[j].database,iNum_rows);  
	//myprintf tables
             k=0;
             while((g_row=mysql_fetch_row(g_res)))
             {
                strcpy(DataBaseInfo[j].tables[k].tablename,g_row[0]);
                myprintf("%s\n",g_row[0]);
                k++;
             }
             DataBaseInfo[j].dbtable_num=k;
             mysql_free_result(g_res);

        /*********get maxid *********/

            myprintf("\nmaxid:\t");
            for(k=0;k<DataBaseInfo[j].dbtable_num;k++)
            {
                sprintf(sql,"select max(id) from ");
                strcat(sql,DataBaseInfo[j].tables[k].tablename);
                if(executesql(sql))
                {
                    print_mysql_error(NULL);
                    DataBaseInfo[j].tables[k].maxid=0;
                    continue;
                }

                g_res=mysql_use_result(g_conn);
                g_row=mysql_fetch_row(g_res);
                if(g_row[0]=='\0') 
                {
                   DataBaseInfo[j].tables[k].maxid=0;    
                }
                else
                {
                   DataBaseInfo[j].tables[k].maxid=atoi(g_row[0]); 
                }
                myprintf("%d\t",DataBaseInfo[j].tables[k].maxid);
                mysql_free_result(g_res);
            }
            myprintf("\n");
            mysql_close(g_conn);
        /*********get psid *********/

            if(check_file_exist(DataBaseInfo[j].Idbackpath)==0)//id file exist
            {
                //load file and psid
                FILE *fp=NULL;char buf[20],*p=NULL;
                if((fp=fopen(DataBaseInfo[j].Idbackpath,"r"))==NULL)
                {
                    myprintf("Fail to open file %s...\n",DataBaseInfo[j].Idbackpath);
                    return -1;
                }
                else
                {
                    k=0;
                    myprintf("psid:\t");
                    while((p=fgets(buf,1024,fp))) 
                    {
                        DataBaseInfo[j].tables[k].psid=atoi(p);
                        k++;
                        if(k>=DataBaseInfo[j].dbtable_num) break;
                    }
                    fclose(fp);
                    for(k=0;k<DataBaseInfo[j].dbtable_num;k++)
                    {
                        myprintf("%d\t",DataBaseInfo[j].tables[k].psid);
                    }
                    myprintf("\n");
                }

                /*scan every table and backup*/
                char* start = (char *)malloc(sizeof(int) + 1);  //分配动态内存
                char* end = (char *)malloc(sizeof(int) + 1);  //分配动态内存
                char* filetotalname = (char *)malloc(1024);  //分配动态内存
                char* dirname = (char *)malloc(1024);  //分配动态内存
                for(k=0;k<DataBaseInfo[j].dbtable_num;k++)//scan every table
                {
                //check backup num,num should <= maxrows
                    num=DataBaseInfo[j].tables[k].maxid;
                    if(num <= DataBaseInfo[j].tables[k].psid)
                    {
                        // myprintf("No new data ---%s---\n", DataBaseInfo[j].tables[k].tablename);
                        continue;
                    }
                    else
                    {
                        if(num-DataBaseInfo[j].tables[k].psid > maxrows)
                        {
                            num=DataBaseInfo[j].tables[k].psid+maxrows;
                        } 
                    }

                    //begin to backup
                    memset(start,0,sizeof(int)+1);//alloc memory
                    memset(end,0,sizeof(int)+1);
                    memset(filetotalname,0,1024);//init memory
                    memset(dirname,0,1024);

                    sprintf(start,"%d",DataBaseInfo[j].tables[k].psid+1); //init to string
                    sprintf(end,"%d",num);

                    create_file_name(filetotalname,dirname,DataBaseInfo[j].database,DataBaseInfo[j].tables[k].tablename,start,end,backpath);
                    backup_table_data(DataBaseInfo[j].host,DataBaseInfo[j].username,DataBaseInfo[j].password,DataBaseInfo[j].database,DataBaseInfo[j].tables[k].tablename,start,end,filetotalname,dirname);
                    DataBaseInfo[j].tables[k].psid=num;//update psid
                }

                //backup complete,write new psid to file
                    if((fp=fopen(DataBaseInfo[j].Idbackpath,"w"))==NULL)
                    {
                        myprintf("open file %s fail \n",DataBaseInfo[j].Idbackpath);
                        return -1;
                    }
                    else
                    {
                        for(k=0;k<DataBaseInfo[j].dbtable_num;k++) 
                        {
                            memset(end,0,sizeof(int)+1);
                            sprintf(end,"%d",(DataBaseInfo[j].tables[k].psid));
                            fputs(end,fp);
                            fputs("\n",fp);
                        }
                        fclose(fp);
                    }
                //release memory
                free(start);
                free(end);
                free(filetotalname);
                free(dirname);

            }
            else //id file not exist,psid is all 0
            {
                /*
                * scan every table and backup
                */
                char* start = (char *)malloc(sizeof(int) + 1);  //分配动态内存
                char* end = (char *)malloc(sizeof(int) + 1);  //分配动态内存
                char* filetotalname = (char *)malloc(1024);  //分配动态内存
                char* dirname = (char *)malloc(1024);  //分配动态内存
                FILE *fp=NULL;
                for(k=0;k<DataBaseInfo[j].dbtable_num;k++)//scan every table
                {
                //check backup num,num should <= maxrows
                    num=DataBaseInfo[j].tables[k].maxid;
                    if(num <= 0)
                    {
                        continue;
                    }
                    else
                    {
                        if(num > maxrows)
                        {
                            num=maxrows;
                        } 
                    }

                    //begin to backup
                    memset(start,0,sizeof(int)+1);//alloc memory
                    memset(end,0,sizeof(int)+1);
                    memset(filetotalname,0,1024);//init memory
                    memset(dirname,0,1024);

                    sprintf(start,"%d",0); //init to string
                    sprintf(end,"%d",num);

                    create_file_name(filetotalname,dirname,DataBaseInfo[j].database,DataBaseInfo[j].tables[k].tablename,start,end,backpath);
                    backup_table_data(DataBaseInfo[j].host,DataBaseInfo[j].username,DataBaseInfo[j].password,DataBaseInfo[j].database,DataBaseInfo[j].tables[k].tablename,start,end,filetotalname,dirname);
                    DataBaseInfo[j].tables[k].psid=num;//update psid
                }

                //backup complete,write new psid to file
                    if((fp=fopen(DataBaseInfo[j].Idbackpath,"w"))==NULL)
                    {
                        myprintf("open file %s fail\n",DataBaseInfo[j].Idbackpath);
                        return -1;
                    }
                    else
                    {
                        for(k=0;k<DataBaseInfo[j].dbtable_num;k++) 
                        {
                            memset(end,0,sizeof(int)+1);
                            sprintf(end,"%d",(DataBaseInfo[j].tables[k].psid));
                            fputs(end,fp);
                            fputs("\n",fp);
                        }
                        fclose(fp);
                    }
                //release memory
                free(start);
                free(end);
                free(filetotalname);
                free(dirname);

            }
        
        }


    }

    //log rotate 
    xmlChar logfilepath[512]={0};
    strcpy(logfilepath,logpath);
    strcat(logfilepath,".log.txt");
    rotate_log(logfilepath);

    //check harddisk remain size
    xmlChar server_info[512]={0};
    if(check_hddisk_remain()>=70)
    {
        //dump host info
        strcpy(server_info,"echo ");
        strcat(server_info,ServerInfo);
        strcat(server_info," ");
        strcat(server_info," >>");
        strcat(server_info,EmailBodyPath);
        myprintf("server info %s\n",server_info);
        Exec_Shell_Cmd(server_info);

        strcpy(server_info,"fdisk -l >> ");
        strcat(server_info,EmailBodyPath);
        Exec_Shell_Cmd(server_info);
        
        usleep(100*1000);
        Exec_Shell_Cmd(SendEmailCMD);
        myprintf("%s\n",SendEmailCMD);
        myprintf("HardDisk remain lessthen 30!!!");
        myprintf("I Have sent mail to Master");
        myprintf("\n");
        fsync(logfile_num);
        fclose(logfile);
        exit(1);
    }

    //check if database has no data per day,send mail to master
    if(check_file_exist("/home/asusftp/.empty")==0 && isempty==0)
    {
        //dump host info
        strcpy(server_info,"echo ");
        strcat(server_info,ServerInfo);
        strcat(server_info," ");
        strcat(server_info," >>");
        strcat(server_info,EmailBodyPath);
        myprintf("server info %s\n",server_info);
        Exec_Shell_Cmd(server_info);

        strcpy(server_info,"echo /*No Data Today/* >> ");
        strcat(server_info,EmailBodyPath);
        Exec_Shell_Cmd(server_info);
        
        usleep(100*1000);
        Exec_Shell_Cmd(SendEmailCMD);
        myprintf("%s\n",SendEmailCMD);
        myprintf("No Data Found Today!");
        myprintf("I Have sent mail to Master");
        myprintf("\n");
        isempty=1;
    }

    //chek if stop flag is on
    xmlChar stop_flag_file[512]={0};
    strcpy(stop_flag_file,backpath);
    strcat(stop_flag_file,".stop");
    if(check_file_exist(stop_flag_file)==0)
    {
        myprintf("stop flag has been setup,stop....\n");
        printf("stop flag has been setup,stop....\n");
        fsync(logfile_num);
        fclose(logfile);
        exit(0);
    }

    // sleep interval msec
    myprintf("\n\n\n****************************************************\n");
    myprintf("*     Wait %d seconds before next loop             *\n", interval/1000);
    myprintf("*     Now you can cancel backup by --Ctrl+c--      *\n");
    myprintf("****************************************************\n\n\n");
    usleep(interval*1000);
}

fsync(logfile_num);
fclose(logfile);
exit(0);

}

