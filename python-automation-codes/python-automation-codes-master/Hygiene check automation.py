# -*- coding: utf-8 -*-
"""
Created on Fri Jul 26 12:36:11 2019

@author: imart
"""
#--------------------------------------ADD--------------------------------------



error = []
df= pd.read_excel('file:///C:/Users/imart/Downloads/IsqBulkAdditionSample (63).xls',encoding = "ISO-8859-1")

if(len(df.columns)!=14):
    error.append('Sheet is not having all columns')

df['IM_SPEC_MASTER_DESC'] = df['IM_SPEC_MASTER_DESC'].apply(lambda x:x.title())    
df['IM_SPEC_OPTIONS_DESC'] = df['IM_SPEC_OPTIONS_DESC'].apply(lambda x:x.title()) 
#spellcheck
wordlist = open('C:/Users/imart/Downloads/english3/english3.txt').read().splitlines() #difflib



for i in range (0,len(df)):
        words1 = map(lambda x: x.strip(), wordlist)
        s=df.IM_SPEC_MASTER_DESC[i].lower() in words1   
        if(str(s)== 'False'):
            list1=[]
            list1 =df.IM_SPEC_MASTER_DESC[i].split(' ')
            for z in list1:
                    words1 = map(lambda x: x.strip(), wordlist)
                    p=z.lower() in words1
                    if(str(p)== 'False'):
                        error.append('Spelling of ISQ is wrong at row '+str(i+2) +"word = " + z.lower())
                        break
  
    
#import difflib                     
#s = difflib.get_close_matches('quanity'.lower(), wordlist, n=5, cutoff=0.8)                    

for i in range (0,len(df)):
        words1 = map(lambda x: x.strip(), wordlist)
        s=df.IM_SPEC_OPTIONS_DESC[i].lower() in words1   
        if(str(s)== 'False'):
            list1=[]
            list1 =df.IM_SPEC_OPTIONS_DESC[i].split(' ')
            for z in list1:
                    words1 = map(lambda x: x.strip(), wordlist)
                    p=z.lower() in words1
                    if(str(p)== 'False'):
                        error.append('Spelling of Option desc is wrong at row '+str(i+2) +"word = " + z.lower() )
                        break
  
    

wordlist.index_value()

#other correction
    
for i in range(0,len(df)):
    if(df.IM_SPEC_OPTIONS_DESC[i]=='Others'):
        df.IM_SPEC_OPTIONS_DESC[i]='Other'
        
        
for i in range(0,len(df)):
    if(df.IM_SPEC_OPTIONS_DESC[i]=='Other'):
            df.OPT_SUP_PRIORITY[i] = 99
            
        
        
#special charac correction
import re         
ditits_and_num1 = r'@[A-Za-z0-9_]+'
hyperlink_2 = r'https?://[^ ]+'
numeric_3 = r'[0-9]+'
combined_pattern1 = r'|'.join((ditits_and_num1, hyperlink_2,numeric_3))
www_pattern = r'www.[^ ]+'
pat_3 = r'[^A-Za-z0-9]+'

def clean(text):
    cleanr = re.compile('<.*?>')
    text = re.sub(cleanr, '', text)
    text = re.sub(r'[^\w\s]'," ",text)
    text = re.sub("@"," ",text)
    text = re.sub("[0-9] \\w+ *"," ",text)
    text = re.sub("[0-9] \\w+ *"," ",text)
    text = re.sub(" +"," ",text)   
    return text

df['IM_SPEC_OPTIONS_DESC'] = df['IM_SPEC_OPTIONS_DESC'].apply(lambda x:clean(x))            
df['IM_SPEC_MASTER_DESC'] = df['IM_SPEC_MASTER_DESC'].apply(lambda x:clean(x)) 


#priority can not be zero
if(0 in df.IM_CAT_SPEC_PRIORITY.unique() or 0 in df.SUP_PRIORITY.unique()):
    error.append('Priority can not be zero')


#why do your need this
for i in range(0,len(df)):
    if(df.IM_SPEC_MASTER_DESC[i]=='Why do you need this'):
            df.OPT_SUP_PRIORITY[i] = 999
                       
            
#Quantity unit and AOV is dropdown
for i in range(0,len(df)):
    if(df.IM_SPEC_MASTER_DESC[i]=='Quantity Unit'):
            df.IM_SPEC_MASTER_TYPE[i] = 3
                                   
for i in range(0,len(df)):
    if(df.IM_SPEC_MASTER_DESC[i]=='Approximate Order Value'):
            df.IM_SPEC_MASTER_TYPE[i] = 3            
     
df.columns       


#-------------------------------------ISQEDIT--------------------------------------


df= pd.read_excel('file:///C:/Users/imart/Downloads/UploadFileISQBu (1).xls',encoding = "ISO-8859-1")

error = []
if(len(df.columns)!=22):
    error.append('Sheet is not having all columns')

df.columns
#PDN
for i in range(0,len(df)):
    if(df.SPEC_CONFIG_TYPE[i] == 'K' or df.NEW_SPEC_CONFIG_TYPE[i] == 'K' or df.NEW_SPEC_CONFIG_TYPE[i] == 'C' or df.NEW_SPEC_CONFIG_TYPE[i] == 'C'):
        if(df.ISQ_Name[i] == 'Brand'):
            df.NEW_AFFIX_FLAG[i] = 'Prefix'
            df.NEW_AFFIX_DISPLAY_FLAG[i] = 'No'
            
        if(df.ISQ_Name[i] == 'Location' or df.ISQ_Name[i] == 'Service Location/City' or df.ISQ_Name[i] == 'Service Location'):
            df.NEW_AFFIX_FLAG[i] = 'Suffix'
            df.NEW_AFFIX_DISPLAY_FLAG[i] = 'Yes'
            
        if(df.ISQ_Name[i] == 'Size'):
            df.NEW_AFFIX_FLAG[i] = 'Suffix'
            df.NEW_AFFIX_DISPLAY_FLAG[i] = 'Yes'
            
        if(df.ISQ_Name[i] == 'Packaging Type' or df.ISQ_Name[i] == 'Packaging Size' or  df.ISQ_Name[i] == 'Pack Size' or  df.ISQ_Name[i] == 'Pack Type' or  df.ISQ_Name[i] == 'Packing Size' or  df.ISQ_Name[i] == 'Packing Type'):
            df.NEW_AFFIX_FLAG[i] = 'Suffix'
            df.NEW_AFFIX_DISPLAY_FLAG[i] = 'Yes'
            
        if(df.ISQ_Name[i] == 'Grade'):
            df.NEW_AFFIX_FLAG[i] = 'Suffix'
            df.NEW_AFFIX_DISPLAY_FLAG[i] = 'Yes'
            
        if(df.ISQ_Name[i] == 'Capacity'):
            df.NEW_AFFIX_FLAG[i] = 'Suffix'
            df.NEW_AFFIX_DISPLAY_FLAG[i] = 'Yes'
            
        if(df.ISQ_Name[i] == 'Features'):
            df.NEW_AFFIX_FLAG[i] = 'Suffix'
            df.NEW_AFFIX_DISPLAY_FLAG[i] = 'Yes'
            
        if(df.ISQ_Name[i] == 'Warranty'):
            df.NEW_AFFIX_FLAG[i] = 'Suffix'
            df.NEW_AFFIX_DISPLAY_FLAG[i] = 'Yes'


#key always p_s
df.columns   
for i in range(0,len(df)):
    if(df.SPEC_CONFIG_TYPE[i] == 'K' or df.NEW_SPEC_CONFIG_TYPE[i] == 'K' or df.NEW_SPEC_CONFIG_TYPE[i] == 'C' or df.NEW_SPEC_CONFIG_TYPE[i] == 'C'):
               if(str(df.AFFIX_Flag[i]) == 'nan' and str(df.NEW_AFFIX_FLAG[i])== "nan"):
                   error.append('Key Config should be p/s')


