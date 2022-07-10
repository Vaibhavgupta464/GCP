df= pd.read_excel('file:///C:/Users/imart/Desktop/buyer bulk add/seller/add.xlsx')
df1= pd.read_excel('file:///C:/Users/imart/Downloads/IsqBulkAdditionSample (56).xls')



for i in range(0,len(df)):
    if(str(df.MCAT_ID[i])=='nan'):
        df.MCAT_ID[i] = df.MCAT_ID[i-1]

def explode(df, lst_cols, fill_value=''):
    # make sure `lst_cols` is a list
    if lst_cols and not isinstance(lst_cols, list):
        lst_cols = [lst_cols]
    # all columns except `lst_cols`
    idx_cols = df.columns.difference(lst_cols)

    # calculate lengths of lists
    lens = df[lst_cols[0]].str.len()

    if (lens > 0).all():
        # ALL lists in cells aren't empty
        return pd.DataFrame({
            col:np.repeat(df[col].values, lens)
            for col in idx_cols
        }).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
          .loc[:, df.columns]
    else:
        # at least one list in cells is empty
        return pd.DataFrame({
            col:np.repeat(df[col].values, lens)
            for col in idx_cols
        }).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
          .append(df.loc[lens==0, idx_cols]).fillna(fill_value) \
          .loc[:, df.columns]


df=explode(df.assign(Options=df.Options.str.split(',')), 'Options')


df1.columns
df1.IM_CAT_SPEC_CATEGORY_ID = df.MCAT_ID
df1.IM_CAT_SPEC_CATEGORY_TYPE = 3
df1.IM_SPEC_MASTER_BUYER_SELLER = 2
df1.IM_SPEC_MASTER_DESC = df.ISQ
df1.IM_SPEC_MASTER_TYPE = df.ISQ_Type
df1.IM_CAT_SPEC_STATUS =1
df1.IM_SPEC_OPTIONS_DESC = df.Options
df1.IM_SPEC_OPTIONS_STATUS =1
df1.IM_SPEC_OPT_BUYER_SELLER= 0
df1.SPEC_CONFIG_TYPE = df.Key_Config
df1.IM_CAT_SPEC_PRIORITY = df.Seller_Priority
df1.SUP_PRIORITY = df.Seller_Priority

df1=df1.append(pd.Series([np.nan]), ignore_index = True)

df1.reset_index(drop = True,inplace = True)
i=0
row=1
for i in range(0,len(df)):
    if((df1.IM_SPEC_MASTER_DESC[i] == df1.IM_SPEC_MASTER_DESC[i+1]) and (df1.IM_CAT_SPEC_CATEGORY_ID[i]==df1.IM_CAT_SPEC_CATEGORY_ID[i+1])):
        df1.OPT_SUP_PRIORITY[i]= row
        row=row+1
    else:
        df1.OPT_SUP_PRIORITY[i]= row
        row=1


df1.reset_index(drop = True,inplace = True)




df1['IM_SPEC_OPTIONS_DESC']=df1.IM_SPEC_OPTIONS_DESC.astype(str)

df1.columns
for n in range(0,len(df1)):
   df1.set_value(n,'IM_SPEC_OPTIONS_DESC',df1.IM_SPEC_OPTIONS_DESC[n].strip())
   print(n)
df1.reset_index(drop = True,inplace = True)   
   
for i in range(0,len(df)):   
    df1.IM_SPEC_MASTER_DESC[i]=df1.IM_SPEC_MASTER_DESC[i].replace("?","")
df1.reset_index(drop = True,inplace = True)



df1.loc[df1.IM_SPEC_MASTER_TYPE=='Text','IM_SPEC_MASTER_TYPE']=1
df1.loc[df1.IM_SPEC_MASTER_TYPE=='Radio','IM_SPEC_MASTER_TYPE']=2
df1.loc[df1.IM_SPEC_MASTER_TYPE=='Multi Select','IM_SPEC_MASTER_TYPE']=4
df1.loc[df1.IM_SPEC_MASTER_TYPE=='Multiple Select','IM_SPEC_MASTER_TYPE']=4
df1.loc[df1.IM_SPEC_MASTER_TYPE=='DropDown','IM_SPEC_MASTER_TYPE']=3
df1.loc[df1.IM_SPEC_MASTER_TYPE=='Drop Down','IM_SPEC_MASTER_TYPE']=3

df1=df1.drop([0],axis =1)



df1=df1.sort_values(['IM_CAT_SPEC_CATEGORY_ID','IM_CAT_SPEC_PRIORITY','OPT_SUP_PRIORITY'], ascending=True)
df1.reset_index(drop = True,inplace = True)
#-------------------------------------
df1.loc[df1['IM_SPEC_OPTIONS_DESC']=='Others', 'IM_SPEC_OPTIONS_DESC']='Other'
df1.loc[df1['IM_SPEC_OPTIONS_DESC']=='Other', 'OPT_SUP_PRIORITY']=99


df1.reset_index(drop = True,inplace = True)

df1=df1.sort_values(['IM_CAT_SPEC_CATEGORY_ID','IM_CAT_SPEC_PRIORITY','OPT_SUP_PRIORITY'], ascending=True)
df1.reset_index(drop = True,inplace = True)   
print("ok")


#-------------------------------------------------------------

df2= pd.read_excel('file:///C:/Users/imart/Desktop/buyer bulk add/seller/IsqBulkeditSample.xls')

df_new = pd.DataFrame(columns = ['join'])

df['join'] = ""
for i in range(0,len(df)):
    df['join'][i] = str(df.MCAT_ID[i]) + '_' + df.ISQ[i]

df4 = df.drop_duplicates(['join'])


df4 = df4[['S_P_flag', 'Display_flag', 'join']]
df2['join'] = ""
for i in range(0,len(df2)):
    df2['join'][i] = str(df2.MCAT_ID[i]) + '_' + df2.ISQ_Name[i]
    
    
df2= pd.merge(df2,df4,on ='join',how= 'left' )

df2.NEW_AFFIX_FLAG = df2.S_P_flag
df2.NEW_AFFIX_DISPLAY_FLAG = df2.Display_flag

df2.columns
df2.drop(['join', 'S_P_flag', 'Display_flag'],axis = 1,inplace = True)


df2.loc[df2.NEW_AFFIX_FLAG=='P','NEW_AFFIX_FLAG']='Prefix'
df2.loc[df2.NEW_AFFIX_FLAG=='S','NEW_AFFIX_FLAG']='Suffix'


df2=df2.dropna(subset=['NEW_AFFIX_FLAG'])
df2.reset_index(drop = True,inplace = True)

for i in range(0,len(df2)):
    if(df2.AFFIX_Display_Flag[i] == df2.NEW_AFFIX_DISPLAY_FLAG[i]):
        df2.NEW_AFFIX_DISPLAY_FLAG = ""
