About Code: To download Seller ISQ data or Option wise data using MCAT ids to prepare report using tkinter.


--------------------------------------------------------------------------------


import cx_Oracle
import pandas as pd
import tkinter
from tkinter import *
conn_str = '*******'
conn = cx_Oracle.connect(conn_str)

cursor = conn.cursor()

window = Tk()
 
window.title("Welcome")
 
window.geometry('500x500')
 
lbl = Label(window, text="Seller",font=("Arial Bold", 10))

lbl.grid(column=1, row=0)



def isq1():
    
    
    #d = {'col1': [15,17]}
    #df = pd.DataFrame(data=d)
    print("ko")
    df = pd.read_excel('file:///C:/Users/imart/Desktop/data download using python/abc.xlsx')
    masterdf=pd.DataFrame()
    index =0
    dfname=[]
    list = []
    j=0
    k=0
    
    parts=(len(df)//1000)+1
    for i in range (0,parts):
            dfname=df[j:j+999]
            j=j+999
            list.append(dfname)
           
    for k in range (0,parts): 
        df_data =  pd.DataFrame(list[k])        
        querystring = """select IM_CAT_SPEC_CATEGORY_ID MCAT_ID,glcat_mcat_name MCAT_Name,IM_SPEC_MASTER_ID ISQ_ID,IM_SPEC_MASTER_DESC ISQ_Name, null as New_ISQ_Name,
IM_SPEC_MASTER_TYPE ISQ_Type,IM_SPEC_MASTER_FULL_DESC Contextual_Help,
null as New_Contextual_Help,IM_SPEC_MASTER_BUYER_SELLER "Buyer/Seller_Flag",
null as "New_Buyer/Seller_Flag",IM_CAT_SPEC_PRIORITY Buyer_Priority,null as New_Buyer_Priority,IM_CAT_SPEC_SUP_PRIORITY  Seller_Priority
,null as New_Seller_Priority,IM_CAT_SPEC_STATUS STATUS,null as NEW_STATUS,
IM_SPEC_AFFIX_TYPE AFFIX_Flag,null as NEW_AFFIX_FLAG,IM_SPEC_DESC_WITH_AFFIX AFFIX_Display_Flag,
null as NEW_AFFIX_DISPLAY_FLAG,IM_CAT_SPEC_TYPE SPEC_CONFIG_TYPE,null as NEW_SPEC_CONFIG_TYPE
from IM_SPECIFICATION_MASTER a ,im_cat_specification c,glcat_mcat
        where c.FK_IM_SPEC_MASTER_ID = IM_SPEC_MASTER_ID
        and GLCAT_MCAT_ID = IM_CAT_SPEC_CATEGORY_ID
        and IM_SPEC_MASTER_BUYER_SELLER in (0,2)
        and im_cat_spec_status = 1
        and IM_CAT_SPEC_CATEGORY_ID in {}
        """.format(tuple(df_data.MCAT_ID))
        
        df_ora = pd.read_sql(querystring, con=conn)
        
        
        masterdf=pd.concat([masterdf,df_ora],axis=0)
    print("ok")
    masterdf.to_csv('C:/Users/imart/Desktop/data download using python/ISQ_data.csv',index=False)
btn = Button(window, text="ISQ DATA DOWNLOAD", command=isq1)

btn.grid(column=3, row=0)

def option1():
    
    
    #d = {'col1': [15,17]}
    #df = pd.DataFrame(data=d)
    print("ko")
    df = pd.read_excel('file:///C:/Users/imart/Desktop/data download using python/abc.xlsx')
    masterdf1=pd.DataFrame()
    index =0
    dfname=[]
    list = []
    j=0
    k=0
    
    parts=(len(df)//1000)+1
    for i in range (0,parts):
            dfname=df[j:j+999]
            j=j+999
            list.append(dfname)
           
    for k in range (0,parts): 
        df_data =  pd.DataFrame(list[k])        
        querystring = """select IM_CAT_SPEC_CATEGORY_ID MCAT_ID,glcat_mcat_name MCAT_NAME,IM_SPEC_MASTER_ID ISQ_ID,IM_SPEC_MASTER_DESC ISQ_NAME,
    case when IM_SPEC_MASTER_BUYER_SELLER = 2 then 'Supplier'
    when IM_SPEC_MASTER_BUYER_SELLER = 0 then 'Both' end
    "BUYER/SELLER_FLAG",
    case when IM_SPEC_MASTER_TYPE=1 then 'Text'
    when IM_SPEC_MASTER_TYPE=2 then 'Radio' 
    when IM_SPEC_MASTER_TYPE=3 then 'Dropdown' 
    when IM_SPEC_MASTER_TYPE=4 then 'Multiselect' end
    ISQ_TYPE,
    null as NEW_ISQ_TYPE,
    'UPDATE' as OPTION_ACTION,
    IM_SPEC_OPTIONS_ID OPTION_ID,IM_SPEC_OPTIONS_DESC OPTION_DESCRIPTION,
    null as NEW_OPTION_DESCRIPTION,
    case when IM_SPEC_OPTIONS_STATUS = 1 then 'Active' end OPTION_STATUS, null as NEW_OPTION_STATUS,
    case when IM_SPEC_OPT_BUYER_SELLER = 0 then 'Both'
    when IM_SPEC_OPT_BUYER_SELLER = 2 then 'Supplier'
    when IM_SPEC_OPT_BUYER_SELLER = 1 then 'Buyer' end OPTION_FLAG,
    null as NEW_OPTION_FLAG,IM_SPECIFICATION_OPT_PRIORITY OPTION_PRIORITY,
    null as NEW_OPTION_PRIORITY
    from im_cat_specification,im_specification_master,IM_SPECIFICATION_OPTIONS,glcat_mcat
    where IM_CAT_SPECIFICATION.FK_IM_SPEC_MASTER_ID=IM_SPEC_MASTER_ID
    and IM_CAT_SPEC_CATEGORY_TYPE=3
    and glcat_mcat_id = IM_CAT_SPEC_CATEGORY_ID
    and IM_CAT_SPEC_STATUS=1
    and glcat_mcat.GLCAT_MCAT_DELETE_STATUS = 0
    and IM_SPEC_MASTER_BUYER_SELLER in (2,0)
    and IM_CAT_SPECIFICATION.FK_IM_SPEC_MASTER_ID = IM_SPECIFICATION_OPTIONS.FK_IM_SPEC_MASTER_ID
    and IM_SPEC_OPTIONS_STATUS =1
    order by IM_CAT_SPEC_CATEGORY_ID,OPTION_ID
        """.format(tuple(df_data.MCAT_ID))
        
        df_ora = pd.read_sql(querystring, con=conn)
        
        
        masterdf1=pd.concat([masterdf1,df_ora],axis=0)
    print("ok")
    masterdf1.to_csv('C:/Users/imart/Desktop/data download using python/option_data.csv',index=False)
btn1 = Button(window, text="OPTION DATA DOWNLOAD", command=option1)

btn1.grid(column=5, row=0)



window.mainloop()


import datetime
datetime.datetime.now()  
datetime.datetime.now().time()  
str(datetime.now())    

ex=[]
for i in range(0,3):
   ex.append('df'+str(i))

for c in ex:
    exec('{} = pd.DataFrame()'.format(c))    
