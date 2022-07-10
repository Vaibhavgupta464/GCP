import cx_Oracle
import pandas as pd
conn_str = '***********'
conn = cx_Oracle.connect(conn_str)

cursor = conn.cursor()

#----------------------------Top ISQ Availability and Impacting KPIs---------------------------

querystring = """WITH MY_ATTRIBUTES AS    
(    
    SELECT IM_CAT_SPEC_CATEGORY_ID AS GLCAT_MCAT_ID , FK_IM_SPEC_MASTER_ID IM_SPEC_MASTER_ID    
    FROM IM_CAT_SPECIFICATION, IM_SPECIFICATION_MASTER    
    WHERE    
    IM_CAT_SPEC_CATEGORY_TYPE =3 AND IM_CAT_SPEC_STATUS=1    
    AND IM_SPEC_MASTER_BUYER_SELLER  NOT IN (2)    
    AND IM_SPEC_MASTER_DESC NOT     IN ('Quantity Unit','Currency')
    AND FK_IM_SPEC_MASTER_ID = IM_SPEC_MASTER_ID    
),    
MY_OFFERS AS    
(    
    SELECT NULL FK_GL_MODULE_ID, 'APP' STATUS,ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID FROM ETO_OFR 
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_GL_MODULE_ID = 'FENQ'
    --and ETO_OFR_S_IP_COUNTRY in ('India')
    UNION    ALL
    SELECT NULL FK_GL_MODULE_ID, 'APP' STATUS,ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID FROM ETO_OFR_EXPIRED 
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_GL_MODULE_ID = 'FENQ'
    AND ETO_OFR_APPROV = 'A'
    --and ETO_OFR_S_IP_COUNTRY in ('India')
    UNION    ALL
    SELECT NULL FK_GL_MODULE_ID, 'REJ' STATUS,ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID FROM ETO_OFR_EXPIRED 
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_GL_MODULE_ID = 'FENQ'    
    AND ETO_OFR_APPROV <> 'A'   
    --and ETO_OFR_S_IP_COUNTRY in ('India') 
    UNION    ALL
    SELECT NULL FK_GL_MODULE_ID, 'APP' STATUS,ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID FROM ETO_OFR_EXPIRED_ARCH 
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_GL_MODULE_ID = 'FENQ'
    AND ETO_OFR_APPROV = 'A'
    --and ETO_OFR_S_IP_COUNTRY in ('India') 
    UNION    ALL
    SELECT NULL FK_GL_MODULE_ID, 'REJ' STATUS,ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID FROM ETO_OFR_EXPIRED_ARCH 
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_GL_MODULE_ID = 'FENQ'
    AND ETO_OFR_APPROV <> 'A'
    --and ETO_OFR_S_IP_COUNTRY in ('India') 
    UNION    ALL
    SELECT QUERY_MODID FK_GL_MODULE_ID, 'REJ', DIR_QUERY_FREE_REFID , DIR_QUERY_MCATID FROM ETO_OFR_FROM_FENQ 
    WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))  
    AND FK_ETO_OFR_ID IS NULL
    AND QUERY_MODID <> 'INTENT'
    --and S_COUNTRY_UPPER in ('IN')
    UNION    ALL
    SELECT QUERY_MODID FK_GL_MODULE_ID, 'REJ', DIR_QUERY_FREE_REFID , DIR_QUERY_MCATID FROM ETO_OFR_FROM_FENQ_ARCH
    WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))   
    AND FK_ETO_OFR_ID IS NULL
    AND QUERY_MODID <> 'INTENT'
    --and S_COUNTRY_UPPER in ('IN')
    UNION    ALL
    SELECT NULL FK_GL_MODULE_ID, 'REJ', ETO_OFR_DISPLAY_ID , DIR_QUERY_MCATID FROM DIR_QUERY_FREE 
    WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
    AND DIR_QUERY_FREE_BL_TYP <> 1
    AND QUERY_MODID <> 'INTENT'
    --and S_COUNTRY_UPPER in ('IN')
),
MY_FENQ_SOURCE AS ( 
    SELECT FK_ETO_OFR_ID, QUERY_MODID MODID_ORIG, ENQUIRY_SMS_ID FROM ETO_OFR_FROM_FENQ 
    WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
    UNION ALL
    SELECT FK_ETO_OFR_ID, QUERY_MODID MODID_ORIG, ENQUIRY_SMS_ID FROM ETO_OFR_FROM_FENQ_ARCH 
    WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd')) )
select module,
cast(cast((APP_ISQ_QUESTIONS_FILLED/APP_ISQ_QUESTIONS_AVAIL)*100 as numeric (10,2) ) as varchar(10))||'%' as "FENQ_FILLRATE_@App",
cast(cast((GEN_ISQ_QUESTIONS_FILLED_USER/GEN_ISQ_QUESTIONS_AVAIL)*100 as numeric (10,2) ) as varchar(10))||'%' as "FENQ_FILLRATE_@GEN",
GEN_ISQ_QUESTIONS_FILLED_USER/TOTAL_GENERATED as "ISQ_per_FENQ@GEN",
APP_ISQ_QUESTIONS_FILLED/TOTAL_APPROVED as "ISQ_per_FENQ@APP",
TOTAL_GENERATED,
GENERATED_WITH_ISQ_AVAIL,
GEN_ISQ_QUESTIONS_AVAIL,
GEN_ISQ_QUESTIONS_FILLED,
GEN_ISQ_QUESTIONS_FILLED_USER,
TOTAL_APPROVED,
APPROVED_WITH_ISQ_AVAIL,
APPROVED_WITH_ISQ_FILLED,
APP_ISQ_QUESTIONS_AVAIL,
APP_ISQ_QUESTIONS_FILLED,
APP_ISQ_QUESTIONS_FILLED_USER
from (SELECT  MODULE,    
    COUNT(DISTINCT ETO_OFR_DISPLAY_ID) TOTAL_GENERATED,    
    COUNT(DISTINCT DECODE(IM_SPEC_MASTER_ID,NULL,NULL,ETO_OFR_DISPLAY_ID)) GENERATED_WITH_ISQ_AVAIL,    
    COUNT(DISTINCT DECODE(IM_SPEC_MASTER_ID,NULL,NULL,AVAIL_QUES)) GEN_ISQ_QUESTIONS_AVAIL,    
    COUNT(DISTINCT DECODE(IM_SPEC_MASTER_ID,NULL,NULL,FILLED_QUES)) GEN_ISQ_QUESTIONS_FILLED,    
    COUNT(DISTINCT DECODE(IM_SPEC_MASTER_ID,NULL,NULL,FILLED_QUES_GEN)) GEN_ISQ_QUESTIONS_FILLED_USER,    
    COUNT(DISTINCT DECODE(STATUS,'APP',ETO_OFR_DISPLAY_ID)) TOTAL_APPROVED,    
    COUNT(DISTINCT DECODE(STATUS,'APP',(DECODE(IM_SPEC_MASTER_ID,NULL,NULL,ETO_OFR_DISPLAY_ID)))) APPROVED_WITH_ISQ_AVAIL,    
    COUNT(DISTINCT DECODE(STATUS,'APP',(DECODE(FILLED_QUES,NULL,NULL,ETO_OFR_DISPLAY_ID)))) APPROVED_WITH_ISQ_FILLED,    
    COUNT(DISTINCT DECODE(STATUS,'APP',(DECODE(IM_SPEC_MASTER_ID,NULL,NULL,AVAIL_QUES)))) APP_ISQ_QUESTIONS_AVAIL,    
    COUNT(DISTINCT DECODE(STATUS,'APP',(DECODE(IM_SPEC_MASTER_ID,NULL,NULL,FILLED_QUES)))) APP_ISQ_QUESTIONS_FILLED,    
    COUNT(DISTINCT DECODE(STATUS,'APP',(DECODE(IM_SPEC_MASTER_ID,NULL,NULL,FILLED_QUES_GEN)))) APP_ISQ_QUESTIONS_FILLED_USER    
FROM    
    (     SELECT  STATUS, ETO_OFR_DISPLAY_ID, IM_SPEC_MASTER_ID, ETO_OFR_DISPLAY_ID || '-' || IM_SPEC_MASTER_ID AVAIL_QUES, FK_GLCAT_MCAT_ID ,  DECODE( MODID_ORIG ,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP','FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE
   FROM     MY_ATTRIBUTES, MY_OFFERS, MY_FENQ_SOURCE    WHERE 
        FK_GLCAT_MCAT_ID = GLCAT_MCAT_ID(+)
        AND ETO_OFR_DISPLAY_ID = FK_ETO_OFR_ID(+)
        AND NVL(MODID_ORIG,'FENQ') <> 'INTENT'
    ),    
    ( 
        SELECT DISTINCT ETO_OFR_DISPLAY_ID || '-' || FK_IM_SPEC_MASTER_ID FILLED_QUES FROM
        MY_OFFERS, ETO_ATTRIBUTE
        WHERE ETO_OFR_DISPLAY_ID = FK_ETO_OFR_DISPLAY_ID    
        AND FK_IM_SPEC_MASTER_DESC NOT IN ('Quantity Unit','Currency')
    ) FILLED_QUES_ALL,    
    (    
        SELECT DISTINCT ETO_OFR_DISPLAY_ID || '-' || FK_IM_SPEC_MASTER_ID FILLED_QUES_GEN FROM    
        MY_OFFERS, ETO_ATTRIBUTE,   
        (     
            SELECT FK_ETO_OFR_DISPLAY_ID HIST_DISP_ID ,
            ETO_ATR_COLUMN_VALUE FROM ETO_ATTRIBUTE_HISTORY 
            WHERE (ETO_ATR_HIST_UPDBY_SCREEN NOT IN ('LEAP', 'GLADMIN (EDIT BL ISQ
SCREEN)') or ETO_ATR_HIST_UPDBY_SCREEN is null)
            AND ETO_ATR_COLUMN_VALUE NOT LIKE '%Quantity Unit%' AND 
                  ETO_ATR_COLUMN_VALUE NOT LIKE '%Currency%'
            AND ETO_ATR_COLUMN_VALUE <> 'MCAT' AND ETO_ATR_HIST_TYPE = 'I'     AND ETO_ATR_HIST_OLD_VALUE IS NULL 
        )    
        WHERE ETO_OFR_DISPLAY_ID = FK_ETO_OFR_DISPLAY_ID    
        AND ETO_OFR_DISPLAY_ID = HIST_DISP_ID    
        AND FK_IM_SPEC_MASTER_DESC NOT IN ('Quantity Unit','Currency')
        AND INSTR(ETO_ATR_COLUMN_VALUE,'(' || FK_IM_SPEC_MASTER_ID || ')') > 0    
    ) FILLED_QUES_GEN    
WHERE 
    AVAIL_QUES = FILLED_QUES(+)    
    AND AVAIL_QUES = FILLED_QUES_GEN(+)    
    GROUP BY ROLLUP(MODULE))
        """
        
df_fenq = pd.read_sql(querystring, con=conn)

querystring = """WITH MY_ATTRIBUTES AS
(SELECT IM_CAT_SPEC_CATEGORY_ID AS GLCAT_MCAT_ID ,
COUNT(DISTINCT FK_IM_SPEC_MASTER_ID) NUM_QUESTIONS
FROM IM_CAT_SPECIFICATION,
IM_SPECIFICATION_MASTER
WHERE IM_CAT_SPEC_CATEGORY_TYPE =3
AND IM_CAT_SPEC_STATUS =1
AND IM_SPEC_MASTER_BUYER_SELLER NOT IN (2)
AND IM_SPEC_MASTER_DESC NOT     IN ('Quantity Unit','Currency')
AND FK_IM_SPEC_MASTER_ID = IM_SPEC_MASTER_ID
GROUP BY IM_CAT_SPEC_CATEGORY_ID
),
MY_OFFERS AS
(SELECT
DECODE(FK_GL_MODULE_ID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP',
'FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE,
'APP' STATUS,
ETO_OFR_DISPLAY_ID ,
FK_GLCAT_MCAT_ID
FROM ETO_OFR
WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND FK_GL_MODULE_ID <> 'FENQ'
UNION
SELECT
DECODE(FK_GL_MODULE_ID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP',
'FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE,
'APP' STATUS,
ETO_OFR_DISPLAY_ID ,
FK_GLCAT_MCAT_ID
FROM ETO_OFR_EXPIRED
WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND FK_GL_MODULE_ID <> 'FENQ'
UNION
SELECT
DECODE(FK_GL_MODULE_ID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP',
'FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE,
'REJ' STATUS,
ETO_OFR_DISPLAY_ID ,
FK_GLCAT_MCAT_ID
FROM ETO_OFR_EXPIRED_ARCH
WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND FK_GL_MODULE_ID <> 'FENQ'
UNION
SELECT
DECODE(FK_GL_MODULE_ID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP',
'FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE,
'REJ',
ETO_OFR_DISPLAY_ID ,
FK_GLCAT_MCAT_ID
FROM ETO_OFR_TEMP_DEL
WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND FK_GL_MODULE_ID <> 'FENQ'
UNION
SELECT DECODE(FK_GL_MODULE_ID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP','FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP')
MODULE,'REJ',ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
FROM ETO_OFR_TEMP_DEL_ARCH
WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND FK_GL_MODULE_ID <> 'FENQ'
UNION
SELECT
DECODE(QUERY_MODID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP','FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE,
'PEND',ETO_OFR_DISPLAY_ID ,
DIR_QUERY_MCATID
FROM DIR_QUERY_FREE
WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND QUERY_MODID <> 'FENQ'
AND DIR_QUERY_FREE_BL_TYP = 1
),
MY_GEN_MCAT AS
(SELECT *
FROM
(SELECT FK_ETO_OFR_ID,
ETO_OFR_HIST_NEW_VAL GEN_MCAT,
ROW_NUMBER() OVER(PARTITION BY FK_ETO_OFR_ID ORDER BY
FK_ETO_OFR_HIST_ID) RN
FROM ETO_OFR_HIST_DETAIL A,
ETO_OFR_HIST_MAIN B
WHERE ETO_OFR_HIST_ID = FK_ETO_OFR_HIST_ID
AND ETO_OFR_HIST_FIELD = 'FK_GLCAT_MCAT_ID'
AND (ETO_OFR_HIST_OLD_VAL IS NULL
OR ETO_OFR_HIST_OLD_VAL <=0)
AND ETO_OFR_HIST_NEW_VAL > 0
AND ETO_OFR_HIST_USR_ID IS NOT NULL
AND TRUNC(ETO_OFR_HIST_DATE) >= to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd'))
)
WHERE RN = 1
),
MY_FILLED_RESPS AS
(SELECT FK_ETO_OFR_DISPLAY_ID RESP_DISPLAY_ID,
COUNT(DISTINCT ETO_ATR_COLUMN_VALUE) FILLED_RESPONSES,
COUNT(DISTINCT
CASE
WHEN ETO_ATR_COLUMN_VALUE LIKE '%(-1)'
THEN ETO_ATR_COLUMN_VALUE
END ) CUSTOM_FILLED,
COUNT(DISTINCT
CASE
WHEN ETO_ATR_COLUMN_VALUE NOT LIKE '%(-1)'
THEN ETO_ATR_COLUMN_VALUE
END ) REGULAR_FILLED
FROM ETO_ATTRIBUTE_HISTORY
where (ETO_ATR_HIST_UPDBY_SCREEN NOT IN ('LEAP', 'GLADMIN (EDIT BL ISQ
SCREEN)') or ETO_ATR_HIST_UPDBY_SCREEN is null)
AND ETO_ATR_COLUMN_VALUE NOT LIKE '%Quantity Unit%' AND 
ETO_ATR_COLUMN_VALUE NOT LIKE '%Currency%'
AND ETO_ATR_COLUMN_VALUE <> 'MCAT'
AND ETO_ATR_HIST_TYPE = 'I'
AND ETO_ATR_HIST_OLD_VALUE IS NULL
AND TRUNC(ETO_ATR_HIST_UPD_DATE) >= to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd'))
GROUP BY FK_ETO_OFR_DISPLAY_ID
)
select MODULE, REGULAR_FILLED/AVAIL_QUES "ISQFR_directbl_organic",
REGULAR_FILLED/LEADS_GEN "FILLED_PER_BL",
LEADS_GEN,
LEADS_GEN_WITH_ISQ,
LEADS_WITH_FILLED_ISQ,
AVAIL_QUES,
AVAIL_QUES_CUST,
FILLED_RESPONSES,
CUSTOM_FILLED,
REGULAR_FILLED
from (
SELECT MODULE,
COUNT(DISTINCT ETO_OFR_DISPLAY_ID) LEADS_GEN,
COUNT(DISTINCT DECODE(GLCAT_MCAT_ID,NULL,NULL,ETO_OFR_DISPLAY_ID)) LEADS_GEN_WITH_ISQ,
COUNT(DISTINCT DECODE(GEN_MCAT,-1,NULL,NULL,NULL,DECODE(FILLED_RESPONSES,NULL,NULL,ETO_OFR_DISPLAY_ID))) LEADS_WITH_FILLED_ISQ,
SUM(NUM_QUESTIONS) AVAIL_QUES,
SUM(NUM_QUESTIONS) + SUM(DECODE(GEN_MCAT,-1,0,NULL,0,CUSTOM_FILLED)) AVAIL_QUES_CUST,
SUM(DECODE(GEN_MCAT,-1,0,NULL,0,FILLED_RESPONSES)) FILLED_RESPONSES,
SUM(DECODE(GEN_MCAT,-1,0,NULL,0,CUSTOM_FILLED)) CUSTOM_FILLED,
SUM(DECODE(GEN_MCAT,-1,0,NULL,0,REGULAR_FILLED)) REGULAR_FILLED
FROM MY_OFFERS,
MY_GEN_MCAT,
MY_ATTRIBUTES,
MY_FILLED_RESPS
WHERE ETO_OFR_DISPLAY_ID = FK_ETO_OFR_ID(+)
AND GEN_MCAT = GLCAT_MCAT_ID(+)
AND ETO_OFR_DISPLAY_ID = RESP_DISPLAY_ID(+)
GROUP BY ROLLUP (MODULE))

            """
            
df_DirectBL = pd.read_sql(querystring, con=conn)

querystring = """WITH MY_ATTRIBUTES AS
(SELECT GLCAT_MCAT_ID ,
IM_SPEC_MASTER_ID
FROM IM_CAT_SPECIFICATION@IMBLR A,
GLCAT_MCAT@IMBLR B ,
IM_SPECIFICATION_MASTER@IMBLR
WHERE IM_CAT_SPEC_CATEGORY_ID = GLCAT_MCAT_ID
AND IM_CAT_SPEC_CATEGORY_TYPE =3
AND IM_SPEC_MASTER_DESC NOT     IN ('Quantity Unit','Currency')
AND IM_CAT_SPEC_STATUS =1
AND FK_IM_SPEC_MASTER_ID = IM_SPEC_MASTER_ID
AND IM_SPEC_MASTER_BUYER_SELLER NOT IN (2)
),
MY_QUERIES AS
(SELECT QUERY_MODID,
QUERY_ID,
DIR_QUERY_MCATID
FROM DIR_QUERY@ENQDBR
WHERE TRUNC(DATE_R) between to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
--WHERE TRUNC(DATE_R) BETWEEN '1-DEC-2017' AND '10-DEC-2017'
AND QUERY_MODID NOT IN ('ASTBUY','TDW','PNSBLENQ','BUYRCALL','WSITE','PCAT')
)
SELECT MODULE,
COUNT(DISTINCT QUERY_ID) TOTAL_ENQUIRY,
COUNT(DISTINCT DECODE(IM_SPEC_MASTER_ID,NULL,NULL,QUERY_ID))
ENQUIRY_WITH_ISQ,
COUNT(DISTINCT DECODE(FILLED_QUES,NULL,NULL,QUERY_ID))
ENQUIRY_WITH_ISQ_FILLED,
COUNT(DISTINCT DECODE(IM_SPEC_MASTER_ID,NULL,NULL,AVAIL_QUES))
TOTAL_DIST_QUESTIONS,
COUNT(DISTINCT
DECODE(IM_SPEC_MASTER_ID,NULL,NULL,DECODE(QTYPE,1,AVAIL_QUES)))
TOTAL_DIST_QUESTIONS_REG,
COUNT(DISTINCT
DECODE(IM_SPEC_MASTER_ID,NULL,NULL,DECODE(QTYPE,2,AVAIL_QUES)))
TOTAL_DIST_QUESTIONS_CUST,
COUNT(DISTINCT DECODE(IM_SPEC_MASTER_ID,NULL,NULL,FILLED_QUES))
TOTAL_FILLED_QUES,
COUNT(DISTINCT
DECODE(IM_SPEC_MASTER_ID,NULL,NULL,DECODE(QTYPE,1,FILLED_QUES)))
TOTAL_FILLED_QUES_REG,
COUNT(DISTINCT
DECODE(IM_SPEC_MASTER_ID,NULL,NULL,DECODE(QTYPE,2,FILLED_QUES)))
TOTAL_FILLED_QUES_CUST
FROM
(SELECT
DECODE(QUERY_MODID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP','FUS
IONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE,
QUERY_ID,
IM_SPEC_MASTER_ID,
QUERY_ID
|| '-'
|| IM_SPEC_MASTER_ID AVAIL_QUES,
DIR_QUERY_MCATID ,
1 QTYPE
FROM MY_ATTRIBUTES,MY_QUERIES
WHERE DIR_QUERY_MCATID = GLCAT_MCAT_ID(+)
UNION
SELECT
DECODE(QUERY_MODID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP','FUS
IONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE,
QUERY_ID,FK_IM_SPEC_MASTER_ID,QUERY_ID|| '-'|| ETO_ATTRIBUTE_ID AVAIL_QUES,DIR_QUERY_MCATID ,2 QTYPE
FROM ETO_ATTRIBUTE@ENQDBR,MY_QUERIES
WHERE QUERY_ID = FK_ETO_OFR_DISPLAY_ID
AND FK_IM_SPEC_MASTER_DESC NOT IN ('Quantity Unit','Currency')
AND FK_IM_SPEC_MASTER_ID = -1
),
( SELECT DISTINCT QUERY_ID|| '-'||DECODE(FK_IM_SPEC_MASTER_ID,-1,ETO_ATTRIBUTE_ID,FK_IM_SPEC_MASTER_ID)FILLED_QUES
FROM MY_QUERIES ,ETO_ATTRIBUTE@ENQDBR
WHERE QUERY_ID = FK_ETO_OFR_DISPLAY_ID
AND FK_IM_SPEC_MASTER_DESC NOT IN ('Quantity Unit','Currency')
) FILLED_QUES_ALL
WHERE AVAIL_QUES = FILLED_QUES(+)
GROUP BY ROLLUP(MODULE)


            """
            
#df_enq = pd.read_sql(querystring, con=conn)


querystring = """SELECT IM_SPEC_MASTER_DESC, SUM(BUYER_SIDE_ISQ)
FROM 
(
SELECT CASE WHEN IM_SPEC_MASTER_DESC IN ('Usage/Application','Usage','Application') THEN 'Usage/Application'
        WHEN (IM_SPEC_MASTER_DESC LIKE '%Color%' or IM_SPEC_MASTER_DESC like '%Colour%') THEN 'Color'
        WHEN IM_SPEC_MASTER_DESC LIKE '%Brand%' THEN 'Brand'
        WHEN IM_SPEC_MASTER_DESC = 'Quantity' THEN 'Quantity'
        WHEN IM_SPEC_MASTER_DESC = 'Why do you need this' THEN 'Why do you need this'
        WHEN IM_SPEC_MASTER_DESC in ('Approximate Order Value','Total Order Value(Rs)','Total Order Value') THEN 'Approximate Order Value'
        WHEN IM_SPEC_MASTER_DESC LIKE '%Size%' THEN 'Size'
        ELSE 'Rest' END AS IM_SPEC_MASTER_DESC,
COUNT(IM_CAT_SPEC_CATEGORY_ID) BUYER_SIDE_ISQ
FROM IM_SPECIFICATION_MASTER ,
IM_CAT_SPECIFICATION
WHERE IM_SPEC_MASTER_ID = FK_IM_SPEC_MASTER_ID
AND IM_SPEC_MASTER_DESC NOT IN ('Quantity Unit','Currency')
AND IM_CAT_SPEC_CATEGORY_TYPE = 3
AND IM_SPEC_MASTER_BUYER_SELLER <> 2
AND IM_CAT_SPEC_STATUS = 1
GROUP BY IM_SPEC_MASTER_DESC 
order by COUNT(IM_CAT_SPEC_CATEGORY_ID) desc)
GROUP BY IM_SPEC_MASTER_DESC
order by  SUM(BUYER_SIDE_ISQ) desc

            """

df_Top_ISQs_count = pd.read_sql(querystring, con=conn)



print("enter start date")
st=input()
print("enter end date")

end=input()

print("ok")

querystring = """SELECT A.*, TRUNC(SOLD_TRANSACTION)/TRUNC(ISQ_FILLED_APPROVED) SOLD_PER
FROM
(
Select
FK_IM_SPEC_MASTER_DESC,
COUNT(ETO_OFR_DISPLAY_ID) ISQ_FILLED_APPROVED,COUNT(DISTINCT ETO_LEAD_PUR_ID)SOLD_TRANSACTION
    FROM
    (
        SELECT ETO_OFR_DISPLAY_ID,FK_GLCAT_MCAT_ID,ETO_OFR_CALL_DISPOSITION_TYPE,USER_IDENTIFIER_FLAG,FK_GL_MODULE_ID,FK_GL_COUNTRY_ISO S_COUNTRY_UPPER 
        FROM ETO_OFR
        WHERE TRUNC(ETO_OFR_APPROV_DATE_ORIG) between """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """ AND ETO_OFR_APPROV='A'
        UNION
        SELECT ETO_OFR_DISPLAY_ID,FK_GLCAT_MCAT_ID,ETO_OFR_CALL_DISPOSITION_TYPE,USER_IDENTIFIER_FLAG,FK_GL_MODULE_ID,FK_GL_COUNTRY_ISO S_COUNTRY_UPPER 
        FROM ETO_OFR_EXPIRED
        WHERE TRUNC(ETO_OFR_APPROV_DATE_ORIG) between """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """  AND ETO_OFR_APPROV='A'
        )A, ETO_LEAD_PUR_HIST B,
        (SELECT FK_ETO_OFR_DISPLAY_ID,CASE WHEN FK_IM_SPEC_MASTER_DESC IN ('Usage/Application','Usage','Application') THEN 'Usage/Application'
        WHEN (FK_IM_SPEC_MASTER_DESC LIKE '%Color%' or FK_IM_SPEC_MASTER_DESC like '%Colour%') THEN 'Color'
        WHEN FK_IM_SPEC_MASTER_DESC LIKE '%Brand%' THEN 'Brand'
        WHEN FK_IM_SPEC_MASTER_DESC = 'Quantity' THEN 'Quantity'
        WHEN FK_IM_SPEC_MASTER_DESC = 'Why do you need this' THEN 'Why do you need this'
        WHEN FK_IM_SPEC_MASTER_DESC in ('Approximate Order Value','Total Order Value(Rs)','Total Order Value') THEN 'Approximate Order Value'
        WHEN FK_IM_SPEC_MASTER_DESC LIKE '%Size%' THEN 'Size'
        ELSE 'Rest' END AS FK_IM_SPEC_MASTER_DESC,
        ETO_ATTRIBUTE_MCAT_ID
          FROM ETO_ATTRIBUTE
         )C
          WHERE C.ETO_ATTRIBUTE_MCAT_ID = A.FK_GLCAT_MCAT_ID
          AND A.ETO_OFR_DISPLAY_ID = B.FK_ETO_OFR_ID(+)   
          AND FK_ETO_OFR_DISPLAY_ID = A.ETO_OFR_DISPLAY_ID
          GROUP BY FK_IM_SPEC_MASTER_DESC)A


            """ 

df_ISQs_Approved_and_Sold_per = pd.read_sql(querystring, con=conn)


querystring = """SELECT IM_SPEC_MASTER_DESC,   
  COUNT(DISTINCT eto_ofr_display_id) offers,   
  COUNT(DISTINCT filled_id) offers_filled   
FROM   
  (SELECT *   
  FROM   
    (SELECT distinct eto_ofr_display_id ,IM_CAT_SPEC_PRIORITY,(select glcat_mcat_name from glcat_mcat where glcat_mcat_id = fk_glcat_mcat_id) MCAT_NAME,   
      fk_glcat_mcat_id,   
      c,   
      question_id ,   IM_SPEC_MASTER_DESC,   
      eto_ofr_display_id   
      || '-'   
      || Upper(IM_SPEC_MASTER_DESC) || '(' ||question_id ||')' uuid   
    FROM   
     (SELECT
    'APP' STATUS,ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
    FROM ETO_OFR
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """ 
    AND FK_GL_MODULE_ID <> 'FENQ'
    UNION
    SELECT
    'APP' STATUS,ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
    FROM ETO_OFR_EXPIRED
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """
    AND FK_GL_MODULE_ID <> 'FENQ'
    UNION
    SELECT
     'REJ' STATUS, ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
    FROM ETO_OFR_EXPIRED_ARCH
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """
    AND FK_GL_MODULE_ID <> 'FENQ'
    UNION
    SELECT
    'REJ',ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID
    FROM ETO_OFR_TEMP_DEL
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """
    AND FK_GL_MODULE_ID <> 'FENQ'
    UNION
    SELECT 'REJ',ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
    FROM ETO_OFR_TEMP_DEL_ARCH
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """ 
    AND FK_GL_MODULE_ID <> 'FENQ'
    UNION
    SELECT
    'PEND',ETO_OFR_DISPLAY_ID ,DIR_QUERY_MCATID
    FROM DIR_QUERY_FREE
    WHERE TRUNC(DATE_R) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """
    AND QUERY_MODID <> 'FENQ' AND DIR_QUERY_FREE_BL_TYP = 1
    UNION
    SELECT 'APP',FK_ETO_OFR_ID ETO_OFR_DISPLAY_ID, DIR_QUERY_MCATID FK_GLCAT_MCAT_ID FROM ETO_OFR_FROM_FENQ
    WHERE TRUNC(DATE_R) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """
    AND QUERY_MODID <> 'INTENT'
    UNION 
    SELECT 'APP',FK_ETO_OFR_ID ETO_OFR_DISPLAY_ID, DIR_QUERY_MCATID FK_GLCAT_MCAT_ID FROM ETO_OFR_FROM_FENQ_ARCH 
    WHERE TRUNC(DATE_R) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """
    AND QUERY_MODID <> 'INTENT'
  ) all_offers,   
      (SELECT im_cat_spec_category_id AS c ,   IM_CAT_SPEC_PRIORITY,
        FK_IM_SPEC_MASTER_ID question_id,   
        CASE WHEN IM_SPEC_MASTER_DESC IN ('Usage/Application','Usage','Application') THEN 'Usage/Application'
        WHEN (IM_SPEC_MASTER_DESC LIKE '%Color%' or IM_SPEC_MASTER_DESC like '%Colour%') THEN 'Color'
        WHEN IM_SPEC_MASTER_DESC LIKE '%Brand%' THEN 'Brand'
        WHEN IM_SPEC_MASTER_DESC = 'Quantity' THEN 'Quantity'
        WHEN IM_SPEC_MASTER_DESC = 'Why do you need this' THEN 'Why do you need this'
        WHEN IM_SPEC_MASTER_DESC in ('Approximate Order Value','Total Order Value(Rs)','Total Order Value') THEN 'Approximate Order Value'
        WHEN IM_SPEC_MASTER_DESC LIKE '%Size%' THEN 'Size'
        ELSE 'Rest' END AS IM_SPEC_MASTER_DESC   
      FROM im_cat_specification,   
        im_specification_master   
      WHERE im_cat_spec_category_type =3   
    AND IM_SPEC_MASTER_DESC NOT IN ('Quantity Unit','Currency')
    AND FK_IM_SPEC_MASTER_ID        = IM_SPEC_MASTER_ID and IM_CAT_SPEC_STATUS = 1 AND IM_SPEC_MASTER_BUYER_SELLER <> 2  
      ) mcat_questions   
    WHERE c = fk_glcat_mcat_id   
    ),   
    ( SELECT UPPER(CASE WHEN filled_id LIKE '%TOTAL ORDER VALUE(RS)%' THEN REPLACE(filled_id,'TOTAL ORDER VALUE(RS)','APPROXIMATE ORDER VALUE') 
      ELSE filled_id END)filled_id
      FROM (SELECT DISTINCT fk_eto_ofr_display_id   
      || '-'   
      || Upper(ETO_ATR_COLUMN_VALUE) AS filled_id
    FROM ETO_ATTRIBUTE_HISTORY
    WHERE  ETO_ATR_COLUMN_VALUE NOT LIKE '%(-1)'
    AND ETO_ATR_COLUMN_VALUE NOT LIKE '%Quantity Unit%' AND ETO_ATR_COLUMN_VALUE NOT LIKE '%Currency%'
    AND ETO_ATR_COLUMN_VALUE <> 'MCAT'
    AND ETO_ATR_HIST_TYPE = 'I'
    AND ETO_ATR_HIST_UPDBY_NAME = 'User'
    AND (ETO_ATR_HIST_UPDBY_SCREEN NOT IN ('LEAP', 'GLADMIN (EDIT BL ISQ SCREEN)') or ETO_ATR_HIST_UPDBY_SCREEN is null)
    AND ETO_ATR_HIST_OLD_VALUE IS NULL
    AND TRUNC(ETO_ATR_HIST_UPD_DATE) >= """ + " ' " + st + " ' " + """
    )
    ) mcat_filled   
  WHERE uuid            = filled_id(+)   
  )   
GROUP BY IM_SPEC_MASTER_DESC

            """ 

df_organic_Fillr_AOV_Qty_and_Why_do_you_need = pd.read_sql(querystring, con=conn)

     

querystring = """SELECT IM_SPEC_MASTER_DESC,   
    COUNT(DISTINCT eto_ofr_display_id) offers,   
    COUNT(DISTINCT filled_id) offers_filled   
  FROM   
    (SELECT *   
    FROM   
      (SELECT eto_ofr_display_id ,IM_CAT_SPEC_PRIORITY,(select glcat_mcat_name from glcat_mcat where glcat_mcat_id = fk_glcat_mcat_id) MCAT_NAME,   
        fk_glcat_mcat_id,   
        c,   
        question_id ,   
        IM_SPEC_MASTER_DESC,   
        eto_ofr_display_id   
        || '-'   
        || UPPER(IM_SPEC_MASTER_DESC) uuid   
      FROM   
       (SELECT
      'APP' STATUS,ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
      FROM ETO_OFR
      WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """
      AND FK_GL_MODULE_ID <> 'FENQ'
      UNION
      SELECT
      'APP' STATUS,ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
      FROM ETO_OFR_EXPIRED
      WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """
      AND FK_GL_MODULE_ID <> 'FENQ'
      UNION
      SELECT
       'APP' STATUS, ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
      FROM ETO_OFR_EXPIRED_ARCH
      WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """
      AND FK_GL_MODULE_ID <> 'FENQ'
      UNION
      SELECT 'APP',FK_ETO_OFR_ID ETO_OFR_DISPLAY_ID, DIR_QUERY_MCATID FK_GLCAT_MCAT_ID FROM ETO_OFR_FROM_FENQ
      WHERE TRUNC(DATE_R) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """
      AND QUERY_MODID <> 'INTENT'
      UNION 
      SELECT 'APP',FK_ETO_OFR_ID ETO_OFR_DISPLAY_ID, DIR_QUERY_MCATID FK_GLCAT_MCAT_ID FROM ETO_OFR_FROM_FENQ_ARCH 
      WHERE TRUNC(DATE_R) BETWEEN """ + " ' " + st + " ' " + """ and """ + " ' " + end + " ' " + """
      AND QUERY_MODID <> 'INTENT'
    ) all_offers,   
        (SELECT im_cat_spec_category_id AS c ,   IM_CAT_SPEC_PRIORITY,
          FK_IM_SPEC_MASTER_ID question_id,   
          CASE WHEN IM_SPEC_MASTER_DESC IN ('Usage/Application','Usage','Application') THEN 'Usage/Application'
          WHEN (IM_SPEC_MASTER_DESC LIKE '%Color%' or IM_SPEC_MASTER_DESC like '%Colour%') THEN 'Color'
          WHEN IM_SPEC_MASTER_DESC LIKE '%Brand%' THEN 'Brand'
          WHEN IM_SPEC_MASTER_DESC = 'Quantity' THEN 'Quantity'
          WHEN IM_SPEC_MASTER_DESC = 'Why do you need this' THEN 'Why do you need this'
          WHEN IM_SPEC_MASTER_DESC in ('Approximate Order Value','Total Order Value(Rs)','Total Order Value') THEN 'Approximate Order Value'
          WHEN IM_SPEC_MASTER_DESC LIKE '%Size%' THEN 'Size'
          ELSE 'Rest' END AS IM_SPEC_MASTER_DESC      
        FROM im_cat_specification, 
          im_specification_master   
        WHERE im_cat_spec_category_type =3   
      AND IM_SPEC_MASTER_DESC  NOT in ('Quantity Unit','Currency')
      AND FK_IM_SPEC_MASTER_ID        = IM_SPEC_MASTER_ID and IM_CAT_SPEC_STATUS = 1 AND IM_SPEC_MASTER_BUYER_SELLER <> 2  
        ) mcat_questions   
      WHERE c = fk_glcat_mcat_id   
      ),   
      ( SELECT DISTINCT fk_eto_ofr_display_id  
        || '-'  
        || CASE WHEN FK_IM_SPEC_MASTER_DESC in ('Approximate Order Value','Total Order Value(Rs)','Total Order Value','total order value(rs)') THEN 'APPROXIMATE ORDER VALUE' ELSE UPPER(FK_IM_SPEC_MASTER_DESC) END AS filled_id  
      FROM eto_attribute 
      WHERE FK_IM_SPEC_MASTER_DESC NOT in ('Quantity Unit','Currency')
      --AND FK_IM_SPEC_MASTER_ID = -1
      AND TRUNC(ETO_ATTRIBUTE_MOD_DATE) >= """ + " ' " + st + " ' " + """
      ) mcat_filled   
    WHERE uuid            = filled_id(+)   
    )   
  GROUP BY IM_SPEC_MASTER_DESC

            """ 

df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need = pd.read_sql(querystring, con=conn)

df_final= pd.DataFrame(columns = ['abc'],index=range(1,204))
df_final.abc[113] = df_Top_ISQs_count['SUM(BUYER_SIDE_ISQ)'][3]
df_final.abc[114] = df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[0]/df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[0]
df_final.abc[115] = df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[0]/df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[0]
df_final.abc[116] = df_ISQs_Approved_and_Sold_per.ISQ_FILLED_APPROVED[0]
df_final.abc[117] = df_ISQs_Approved_and_Sold_per.SOLD_PER[0]
df_final.abc[118] =  df_Top_ISQs_count['SUM(BUYER_SIDE_ISQ)'][0]
df_final.abc[119] = df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[3]/df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[3]
df_final.abc[120] = df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[3]/df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[3]
df_final.abc[121] = df_ISQs_Approved_and_Sold_per.ISQ_FILLED_APPROVED[3]
df_final.abc[122] = df_ISQs_Approved_and_Sold_per.SOLD_PER[3]
df_final.abc[123] =  df_Top_ISQs_count['SUM(BUYER_SIDE_ISQ)'][4]
df_final.abc[124] = df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[6]/df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[6]
df_final.abc[125] = df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[6]/df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[6]
df_final.abc[126] = df_ISQs_Approved_and_Sold_per.ISQ_FILLED_APPROVED[6]
df_final.abc[127] = df_ISQs_Approved_and_Sold_per.SOLD_PER[6]
df_final.abc[128] =  df_Top_ISQs_count['SUM(BUYER_SIDE_ISQ)'][2]
df_final.abc[129] = df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[7]/df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[7]
df_final.abc[130] = df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[7]/df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[7]
df_final.abc[131] = df_ISQs_Approved_and_Sold_per.ISQ_FILLED_APPROVED[7]
df_final.abc[132] = df_ISQs_Approved_and_Sold_per.SOLD_PER[7]
df_final.abc[133] =  df_Top_ISQs_count['SUM(BUYER_SIDE_ISQ)'][6]
df_final.abc[134] = df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[2]/df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[2]
df_final.abc[135] = df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[2]/df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[2]
df_final.abc[136] = df_ISQs_Approved_and_Sold_per.ISQ_FILLED_APPROVED[2]
df_final.abc[137] = df_ISQs_Approved_and_Sold_per.SOLD_PER[2]
df_final.abc[138] =  df_Top_ISQs_count['SUM(BUYER_SIDE_ISQ)'][5]
df_final.abc[139] = df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[5]/df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[5]
df_final.abc[140] = df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[5]/df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[5]
df_final.abc[141] = df_ISQs_Approved_and_Sold_per.ISQ_FILLED_APPROVED[5]
df_final.abc[142] = df_ISQs_Approved_and_Sold_per.SOLD_PER[5]
df_final.abc[143] =  df_Top_ISQs_count['SUM(BUYER_SIDE_ISQ)'][7]
df_final.abc[144] = df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[1]/df_organic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[1]
df_final.abc[145] = df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS_FILLED[1]/df_inorganic_Fillr_AOV_Qty_and_Why_do_you_need.OFFERS[1]
df_final.abc[146] = df_ISQs_Approved_and_Sold_per.ISQ_FILLED_APPROVED[1]
df_final.abc[147] = df_ISQs_Approved_and_Sold_per.SOLD_PER[1]

#--------------------------Top ISQ Availability and Impacting KPIs------------------

#--------------------------ISQ Bucket Wise KPIs--------------------------------------

querystring = """select * from (SELECT SPEC_BUCKET,SUM(CNT) as sum
FROM 
(
SELECT case WHEN BUYER_SIDE_ISQ IS NULL THEN '0 SPECS'
   WHEN BUYER_SIDE_ISQ IN (1) THEN '1 SPEC'
 WHEN BUYER_SIDE_ISQ IN (2) THEN '2 SPECS'
  WHEN BUYER_SIDE_ISQ IN (3) THEN '3 SPECS'
   WHEN BUYER_SIDE_ISQ IN (4) THEN '4 SPECS'
    WHEN BUYER_SIDE_ISQ IN (5) THEN '5 SPECS'
    ELSE '>5 SPECS' END AS SPEC_BUCKET,
COUNT(IM_CAT_SPEC_CATEGORY_ID) AS CNT
FROM(
SELECT IM_CAT_SPEC_CATEGORY_ID,
COUNT(1) BUYER_SIDE_ISQ
FROM IM_SPECIFICATION_MASTER ,
IM_CAT_SPECIFICATION
WHERE IM_SPEC_MASTER_ID = FK_IM_SPEC_MASTER_ID
AND IM_SPEC_MASTER_DESC NOT IN ('Quantity Unit','Currency')
AND IM_CAT_SPEC_CATEGORY_TYPE = 3
AND IM_SPEC_MASTER_BUYER_SELLER <> 2
AND IM_CAT_SPEC_STATUS = 1
GROUP BY IM_CAT_SPEC_CATEGORY_ID
)
GROUP BY BUYER_SIDE_ISQ
)
GROUP BY SPEC_BUCKET


union all

select '0 SPEC' as SPEC_BUCKET,COUNT(DISTINCT GLCAT_MCAT_ID) as sum from glcat_mcat
where glcat_mcat_id not in 
(select IM_CAT_SPEC_CATEGORY_ID
from IM_SPECIFICATION_MASTER ,IM_CAT_SPECIFICATION
where IM_SPEC_MASTER_ID = FK_IM_SPEC_MASTER_ID
and IM_CAT_SPEC_CATEGORY_TYPE =3
and IM_CAT_SPEC_STATUS = 1
AND IM_SPEC_MASTER_DESC NOT     IN ('Quantity Unit','Currency')
and IM_SPEC_MASTER_BUYER_SELLER in (0,1))
and GLCAT_MCAT_DELETE_STATUS =0
)
order by 1

            """ 

df_count_spec = pd.read_sql(querystring, con=conn)

x=0
for i in range(0,7):
    df_final.abc[100+x]=df_count_spec.SUM[x]
    x=x+1
 
querystring = """SELECT SPEC_BUCKET,SUM(CNT)
FROM 
(
SELECT CASE WHEN BUYER_SIDE_ISQ_OPTION IN (1,2,3,4) THEN '1 TO 4 SPEC'
    WHEN BUYER_SIDE_ISQ_OPTION IN (5,6,7,8,9) THEN '5 TO 9 SPEC'
    WHEN BUYER_SIDE_ISQ_OPTION > 9 THEN '>9 SPEC' ELSE '0 SPECS' END AS SPEC_BUCKET,
COUNT(IM_SPEC_MASTER_ID) AS CNT
FROM(SELECT IM_SPEC_MASTER_ID,
    COUNT(1) BUYER_SIDE_ISQ_OPTION
  FROM IM_SPECIFICATION_MASTER,
   IM_SPECIFICATION_OPTIONS C
  WHERE  IM_SPEC_MASTER_ID IN (SELECT FK_IM_SPEC_MASTER_ID FROM IM_CAT_SPECIFICATION WHERE IM_CAT_SPEC_STATUS = 1 AND IM_CAT_SPEC_CATEGORY_TYPE= 3 AND IM_SPEC_MASTER_ID = FK_IM_SPEC_MASTER_ID)
  AND IM_SPEC_MASTER_DESC NOT IN ('Quantity','Quantity Unit','Currency','Approximate Order Value','Usage/Application')
  AND IM_SPEC_MASTER_ID = C.FK_IM_SPEC_MASTER_ID
  AND IM_SPEC_MASTER_TYPE != 1
  AND IM_SPEC_MASTER_BUYER_SELLER <> 2
  AND IM_SPEC_OPTIONS_STATUS = 1
  GROUP BY IM_SPEC_MASTER_ID
)
GROUP BY BUYER_SIDE_ISQ_OPTION
)
GROUP BY SPEC_BUCKET
ORDER BY 1


            """ 

df_spec_over_option = pd.read_sql(querystring, con=conn)

df_final.abc[108]=df_spec_over_option['SUM(CNT)'][0]
df_final.abc[109]=df_spec_over_option['SUM(CNT)'][1]
df_final.abc[110]=df_spec_over_option['SUM(CNT)'][2]

#--------------------------ISQ Bucket Wise KPIs--------------------------------------


#-----------------------MCAT Level Buyer Specs Coverage-----------------------------

querystring = """SELECT DISTINCT A.NO_OF_ISQS,A. NO_OF_UNQ_ISQS, (TOTAL_MCAT-B.MCATS_ATLEAST1_ISQ)MCATS_HAVIN_ZERO_ISQ, B.MCATS_ATLEAST1_ISQ 
FROM
(SELECT      SUM(COUNT(IM_SPEC_MASTER_DESC)) NO_OF_ISQS,SUM(COUNT(DISTINCT IM_SPEC_MASTER_DESC)) NO_OF_UNQ_ISQS
FROM IM_CAT_SPECIFICATION,
  IM_SPECIFICATION_MASTER,
  GLCAT_MCAT
WHERE IM_CAT_SPEC_STATUS              = 1
AND IM_CAT_SPEC_CATEGORY_TYPE         = 3
AND FK_IM_SPEC_MASTER_ID              = IM_SPEC_MASTER_ID
AND IM_CAT_SPEC_CATEGORY_ID           = GLCAT_MCAT_ID
and IM_SPEC_MASTER_BUYER_SELLER <> 2
GROUP BY IM_SPEC_MASTER_DESC)A,
( SELECT MCATS_ATLEAST1_ISQ FROM (
SELECT    GLCAT_MCAT_ID,  COUNT(IM_SPEC_MASTER_DESC) NO_OF_QUESTIONS, COUNT(1) OVER() MCATS_ATLEAST1_ISQ
FROM IM_CAT_SPECIFICATION,  IM_SPECIFICATION_MASTER,  GLCAT_MCAT
WHERE IM_CAT_SPEC_STATUS              = 1
AND IM_CAT_SPEC_CATEGORY_TYPE         = 3
and IM_SPEC_MASTER_BUYER_SELLER <> 2
AND FK_IM_SPEC_MASTER_ID              = IM_SPEC_MASTER_ID
AND IM_CAT_SPEC_CATEGORY_ID           = GLCAT_MCAT_ID
GROUP BY GLCAT_MCAT_ID
HAVING COUNT(IM_SPEC_MASTER_DESC)>=1))B,
( SELECT COUNT(DISTINCT GLCAT_MCAT_ID) TOTAL_MCAT FROM GLCAT_MCAT WHERE GLCAT_MCAT_DELETE_STATUS=0) C

            """ 

df_spec_coverage = pd.read_sql(querystring, con=conn)

querystring = """SELECT decode(IM_SPEC_MASTER_TYPE,1,'Text',2,'Radio Button',3,'Dropdown',4,'Multiple Select','null') ISQtype,
COUNT(1) FROM IM_CAT_SPECIFICATION, IM_SPECIFICATION_MASTER WHERE IM_CAT_SPEC_STATUS = 1 
AND IM_CAT_SPEC_CATEGORY_TYPE = 3 AND FK_IM_SPEC_MASTER_ID = IM_SPEC_MASTER_ID and IM_SPEC_MASTER_BUYER_SELLER <> 2
GROUP BY decode(IM_SPEC_MASTER_TYPE,1,'Text',2,'Radio Button',3,'Dropdown',4,'Multiple Select','null')
order by 1 desc
            """ 

df_spec_type = pd.read_sql(querystring, con=conn)

df_final.abc[151]=df_spec_coverage.NO_OF_ISQS[0]
df_final.abc[152]=df_spec_coverage.NO_OF_ISQS[0]/df_spec_coverage.MCATS_ATLEAST1_ISQ[0]
df_final.abc[153]=df_spec_coverage.NO_OF_UNQ_ISQS[0]
df_final.abc[154]=df_spec_coverage.MCATS_HAVIN_ZERO_ISQ[0]
df_final.abc[155]=df_spec_coverage.MCATS_ATLEAST1_ISQ[0]
df_final.abc[156]=df_spec_type['COUNT(1)'][0]+df_spec_type['COUNT(1)'][1]+df_spec_type['COUNT(1)'][2]+df_spec_type['COUNT(1)'][3]
df_final.abc[157]=df_spec_type['COUNT(1)'][0]
df_final.abc[158]=df_spec_type['COUNT(1)'][1]
df_final.abc[159]=df_spec_type['COUNT(1)'][3]
df_final.abc[160]=df_spec_type['COUNT(1)'][2]

#-----------------------MCAT Level Buyer Specs Coverage-----------------------------


#-----------------------Specs Available Over MCATs-------------------------------


querystring = """WITH MY_ATTRIBUTES AS    
  (    
    SELECT IM_CAT_SPEC_CATEGORY_ID AS GLCAT_MCAT_ID , FK_IM_SPEC_MASTER_ID IM_SPEC_MASTER_ID    
    FROM IM_CAT_SPECIFICATION, IM_SPECIFICATION_MASTER    
    WHERE    
    IM_CAT_SPEC_CATEGORY_TYPE =3 AND IM_CAT_SPEC_STATUS=1    
    AND IM_SPEC_MASTER_BUYER_SELLER  NOT IN (2)    
    AND IM_SPEC_MASTER_DESC NOT     IN ('Quantity Unit','Currency')
    AND FK_IM_SPEC_MASTER_ID = IM_SPEC_MASTER_ID    
),    
MY_OFFERS AS    
(    
    SELECT NULL FK_GL_MODULE_ID, 'APP' STATUS,ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID FROM ETO_OFR 
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_GL_MODULE_ID = 'FENQ'
    UNION    ALL
    SELECT NULL FK_GL_MODULE_ID, 'APP' STATUS,ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID FROM ETO_OFR_EXPIRED 
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_GL_MODULE_ID = 'FENQ'
    AND ETO_OFR_APPROV = 'A'
    UNION    ALL
    SELECT NULL FK_GL_MODULE_ID, 'REJ' STATUS,ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID FROM ETO_OFR_EXPIRED 
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_GL_MODULE_ID = 'FENQ'    
    AND ETO_OFR_APPROV <> 'A'    
    UNION    ALL
    SELECT NULL FK_GL_MODULE_ID, 'APP' STATUS,ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID FROM ETO_OFR_EXPIRED_ARCH 
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_GL_MODULE_ID = 'FENQ'
    AND ETO_OFR_APPROV = 'A'
    UNION    ALL
    SELECT NULL FK_GL_MODULE_ID, 'REJ' STATUS,ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID FROM ETO_OFR_EXPIRED_ARCH 
    WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_GL_MODULE_ID = 'FENQ'
    AND ETO_OFR_APPROV <> 'A'
    UNION    ALL
    SELECT QUERY_MODID FK_GL_MODULE_ID, 'REJ', DIR_QUERY_FREE_REFID , DIR_QUERY_MCATID FROM ETO_OFR_FROM_FENQ 
    WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_ETO_OFR_ID IS NULL
    AND QUERY_MODID <> 'INTENT'
    UNION    ALL
    SELECT QUERY_MODID FK_GL_MODULE_ID, 'REJ', DIR_QUERY_FREE_REFID , DIR_QUERY_MCATID FROM ETO_OFR_FROM_FENQ_ARCH 
    WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND FK_ETO_OFR_ID IS NULL
    AND QUERY_MODID <> 'INTENT'
    UNION    ALL
    SELECT NULL FK_GL_MODULE_ID, 'REJ', ETO_OFR_DISPLAY_ID , DIR_QUERY_MCATID FROM DIR_QUERY_FREE 
    WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))    
    AND DIR_QUERY_FREE_BL_TYP <> 1
    AND QUERY_MODID <> 'INTENT'
),
MY_FENQ_SOURCE AS ( 
    SELECT FK_ETO_OFR_ID, QUERY_MODID MODID_ORIG, ENQUIRY_SMS_ID FROM ETO_OFR_FROM_FENQ
    WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
    UNION ALL
    SELECT FK_ETO_OFR_ID, QUERY_MODID MODID_ORIG, ENQUIRY_SMS_ID FROM ETO_OFR_FROM_FENQ_ARCH 
    WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
    )
      SELECT SPEC_BUCKET,
      cast(cast((APP_ISQ_QUESTIONS_FILLED/APP_ISQ_QUESTIONS_AVAIL)*100 as numeric (10,2) ) as varchar(10))||'%' as "FENQ_FILLRATE_@App",
      cast(cast((GEN_ISQ_QUESTIONS_FILLED_USER/GEN_ISQ_QUESTIONS_AVAIL)*100 as numeric (10,2) ) as varchar(10))||'%' as "FENQ_FILLRATE_@GEN",
      GEN_ISQ_QUESTIONS_FILLED_USER/TOTAL_GENERATED as "ISQ_per_FENQ@GEN",
      APP_ISQ_QUESTIONS_FILLED/TOTAL_APPROVED as "ISQ_per_FENQ@APP",
      TOTAL_GENERATED,
      GENERATED_WITH_ISQ_AVAIL,
      GEN_ISQ_QUESTIONS_AVAIL,
      GEN_ISQ_QUESTIONS_FILLED,
      GEN_ISQ_QUESTIONS_FILLED_USER,
      TOTAL_APPROVED,
      APPROVED_WITH_ISQ_AVAIL,
      APPROVED_WITH_ISQ_FILLED,
      APP_ISQ_QUESTIONS_AVAIL,
      APP_ISQ_QUESTIONS_FILLED,
      APP_ISQ_QUESTIONS_FILLED_USER
    from (SELECT  SPEC_BUCKET,    
        COUNT(DISTINCT ETO_OFR_DISPLAY_ID) TOTAL_GENERATED,    
        COUNT(DISTINCT DECODE(IM_SPEC_MASTER_ID,NULL,NULL,ETO_OFR_DISPLAY_ID)) GENERATED_WITH_ISQ_AVAIL,    
        COUNT(DISTINCT DECODE(IM_SPEC_MASTER_ID,NULL,NULL,AVAIL_QUES)) GEN_ISQ_QUESTIONS_AVAIL,    
        COUNT(DISTINCT DECODE(IM_SPEC_MASTER_ID,NULL,NULL,FILLED_QUES)) GEN_ISQ_QUESTIONS_FILLED,    
        COUNT(DISTINCT DECODE(IM_SPEC_MASTER_ID,NULL,NULL,FILLED_QUES_GEN)) GEN_ISQ_QUESTIONS_FILLED_USER,    
        COUNT(DISTINCT DECODE(STATUS,'APP',ETO_OFR_DISPLAY_ID)) TOTAL_APPROVED,    
        COUNT(DISTINCT DECODE(STATUS,'APP',(DECODE(IM_SPEC_MASTER_ID,NULL,NULL,ETO_OFR_DISPLAY_ID)))) APPROVED_WITH_ISQ_AVAIL,    
        COUNT(DISTINCT DECODE(STATUS,'APP',(DECODE(FILLED_QUES,NULL,NULL,ETO_OFR_DISPLAY_ID)))) APPROVED_WITH_ISQ_FILLED,    
        COUNT(DISTINCT DECODE(STATUS,'APP',(DECODE(IM_SPEC_MASTER_ID,NULL,NULL,AVAIL_QUES)))) APP_ISQ_QUESTIONS_AVAIL,    
        COUNT(DISTINCT DECODE(STATUS,'APP',(DECODE(IM_SPEC_MASTER_ID,NULL,NULL,FILLED_QUES)))) APP_ISQ_QUESTIONS_FILLED,    
        COUNT(DISTINCT DECODE(STATUS,'APP',(DECODE(IM_SPEC_MASTER_ID,NULL,NULL,FILLED_QUES_GEN)))) APP_ISQ_QUESTIONS_FILLED_USER    
    FROM    
        (     SELECT  STATUS, ETO_OFR_DISPLAY_ID, IM_SPEC_MASTER_ID, ETO_OFR_DISPLAY_ID || '-' || IM_SPEC_MASTER_ID AVAIL_QUES, FK_GLCAT_MCAT_ID ,  
          CASE WHEN BUYER_SIDE_ISQ IS NULL THEN '0 SPECS'
        WHEN BUYER_SIDE_ISQ IN (1) THEN '1 SPEC'
 WHEN BUYER_SIDE_ISQ IN (2) THEN '2 SPECS'
  WHEN BUYER_SIDE_ISQ IN (3) THEN '3 SPECS'
   WHEN BUYER_SIDE_ISQ IN (4) THEN '4 SPECS'
    WHEN BUYER_SIDE_ISQ IN (5) THEN '5 SPECS'
    ELSE '>5 SPECS' END AS SPEC_BUCKET
          FROM
          (
        SELECT IM_CAT_SPEC_CATEGORY_ID,
        COUNT(1) BUYER_SIDE_ISQ
        FROM IM_SPECIFICATION_MASTER ,
        IM_CAT_SPECIFICATION
        WHERE IM_SPEC_MASTER_ID = FK_IM_SPEC_MASTER_ID
        AND IM_SPEC_MASTER_DESC NOT     IN ('Quantity Unit','Currency') 
        AND IM_CAT_SPEC_CATEGORY_TYPE = 3
        AND IM_SPEC_MASTER_BUYER_SELLER <> 2
        AND IM_CAT_SPEC_STATUS = 1
        GROUP BY IM_CAT_SPEC_CATEGORY_ID
        ),     MY_ATTRIBUTES, MY_OFFERS, MY_FENQ_SOURCE    WHERE 
            FK_GLCAT_MCAT_ID = GLCAT_MCAT_ID(+)
            AND ETO_OFR_DISPLAY_ID = FK_ETO_OFR_ID(+)
            AND IM_CAT_SPEC_CATEGORY_ID = FK_GLCAT_MCAT_ID
            AND NVL(MODID_ORIG,'FENQ') <> 'INTENT'
        ),    
        ( 
            SELECT DISTINCT ETO_OFR_DISPLAY_ID || '-' || FK_IM_SPEC_MASTER_ID FILLED_QUES FROM
            MY_OFFERS, ETO_ATTRIBUTE
            WHERE ETO_OFR_DISPLAY_ID = FK_ETO_OFR_DISPLAY_ID AND FK_IM_SPEC_MASTER_DESC NOT IN ('Quantity Unit','Currency')
        ) FILLED_QUES_ALL,    
        (    
            SELECT DISTINCT ETO_OFR_DISPLAY_ID || '-' || FK_IM_SPEC_MASTER_ID FILLED_QUES_GEN FROM    
            MY_OFFERS, ETO_ATTRIBUTE,   
            (     
                SELECT FK_ETO_OFR_DISPLAY_ID HIST_DISP_ID ,
                ETO_ATR_COLUMN_VALUE FROM ETO_ATTRIBUTE_HISTORY 
                WHERE ETO_ATR_HIST_UPDBY_SCREEN NOT IN ('LEAP', 'GLADMIN (EDIT BL ISQ SCREEN)')
            AND (ETO_ATR_COLUMN_VALUE NOT LIKE '%Quantity Unit%' OR 
                  ETO_ATR_COLUMN_VALUE NOT LIKE '%Currency%')                
                AND ETO_ATR_COLUMN_VALUE <> 'MCAT' AND ETO_ATR_HIST_TYPE = 'I' AND ETO_ATR_HIST_OLD_VALUE IS NULL 
            )    
            WHERE ETO_OFR_DISPLAY_ID = FK_ETO_OFR_DISPLAY_ID    
            AND ETO_OFR_DISPLAY_ID = HIST_DISP_ID    
            AND FK_IM_SPEC_MASTER_DESC NOT IN ('Quantity Unit','Currency')
            AND INSTR(ETO_ATR_COLUMN_VALUE,'(' || FK_IM_SPEC_MASTER_ID || ')') > 0    
        ) FILLED_QUES_GEN    
    WHERE 
        AVAIL_QUES = FILLED_QUES(+)    
        AND AVAIL_QUES = FILLED_QUES_GEN(+)    
        GROUP BY ROLLUP(SPEC_BUCKET))

            """ 

df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates = pd.read_sql(querystring, con=conn)

querystring = """WITH MY_ATTRIBUTES AS
(SELECT IM_CAT_SPEC_CATEGORY_ID AS GLCAT_MCAT_ID ,
COUNT(DISTINCT FK_IM_SPEC_MASTER_ID) NUM_QUESTIONS
FROM IM_CAT_SPECIFICATION,IM_SPECIFICATION_MASTER
WHERE IM_CAT_SPEC_CATEGORY_TYPE =3 AND IM_CAT_SPEC_STATUS =1 AND IM_SPEC_MASTER_BUYER_SELLER NOT IN (2)
AND FK_IM_SPEC_MASTER_ID = IM_SPEC_MASTER_ID
AND IM_SPEC_MASTER_DESC NOT     IN ('Quantity Unit','Currency')
GROUP BY IM_CAT_SPEC_CATEGORY_ID
),
MY_OFFERS AS
(SELECT
DECODE(FK_GL_MODULE_ID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP','FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE,
'APP' STATUS,ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
FROM ETO_OFR
WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND FK_GL_MODULE_ID <> 'FENQ'
UNION
SELECT
DECODE(FK_GL_MODULE_ID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP','FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE,
'APP' STATUS,ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
FROM ETO_OFR_EXPIRED
WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND FK_GL_MODULE_ID <> 'FENQ'
UNION
SELECT
DECODE(FK_GL_MODULE_ID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP',
'FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE, 'REJ' STATUS, ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
FROM ETO_OFR_EXPIRED_ARCH
WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND FK_GL_MODULE_ID <> 'FENQ'
UNION
SELECT
DECODE(FK_GL_MODULE_ID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP',
'FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE,'REJ',ETO_OFR_DISPLAY_ID , FK_GLCAT_MCAT_ID
FROM ETO_OFR_TEMP_DEL
WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND FK_GL_MODULE_ID <> 'FENQ'
UNION
SELECT DECODE(FK_GL_MODULE_ID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP','FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP')
MODULE,'REJ',ETO_OFR_DISPLAY_ID ,FK_GLCAT_MCAT_ID
FROM ETO_OFR_TEMP_DEL_ARCH
WHERE TRUNC(ETO_OFR_POSTDATE_ORIG) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND FK_GL_MODULE_ID <> 'FENQ'
UNION
SELECT
DECODE(QUERY_MODID,'IMOB','MOBILE','ANDROID','APP','ANDROID','APP','IMAPP','APP','FUSIONI','APP','FUSIONW','APP','FUSIONB','APP','DESKTOP') MODULE,
'PEND',ETO_OFR_DISPLAY_ID ,DIR_QUERY_MCATID
FROM DIR_QUERY_FREE
WHERE TRUNC(DATE_R) BETWEEN to_date(TRUNC(SYSDATE-6)- to_char(sysdate, 'd')) and to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
AND QUERY_MODID <> 'FENQ' AND DIR_QUERY_FREE_BL_TYP = 1
),
MY_GEN_MCAT AS
(SELECT *
FROM
(SELECT FK_ETO_OFR_ID, ETO_OFR_HIST_NEW_VAL GEN_MCAT,ROW_NUMBER() OVER(PARTITION BY FK_ETO_OFR_ID ORDER BY FK_ETO_OFR_HIST_ID) RN
FROM ETO_OFR_HIST_DETAIL A, ETO_OFR_HIST_MAIN B
WHERE ETO_OFR_HIST_ID = FK_ETO_OFR_HIST_ID
AND ETO_OFR_HIST_FIELD = 'FK_GLCAT_MCAT_ID' AND (ETO_OFR_HIST_OLD_VAL IS NULL OR ETO_OFR_HIST_OLD_VAL <=0)
AND ETO_OFR_HIST_NEW_VAL > 0 AND ETO_OFR_HIST_USR_ID IS NOT NULL AND TRUNC(ETO_OFR_HIST_DATE) >= to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
)
WHERE RN = 1
),
MY_FILLED_RESPS AS
(SELECT FK_ETO_OFR_DISPLAY_ID RESP_DISPLAY_ID,
COUNT(DISTINCT ETO_ATR_COLUMN_VALUE) FILLED_RESPONSES,
COUNT(DISTINCT CASE WHEN ETO_ATR_COLUMN_VALUE LIKE '%(-1)' THEN ETO_ATR_COLUMN_VALUE END ) CUSTOM_FILLED,
COUNT(DISTINCT CASE WHEN ETO_ATR_COLUMN_VALUE NOT LIKE '%(-1)' THEN ETO_ATR_COLUMN_VALUE END ) REGULAR_FILLED
FROM ETO_ATTRIBUTE_HISTORY
WHERE ETO_ATR_HIST_UPDBY_SCREEN NOT IN ('LEAP', 'GLADMIN (EDIT BL ISQ SCREEN)')
AND ETO_ATR_COLUMN_VALUE <> 'MCAT'
AND ETO_ATR_HIST_TYPE = 'I'
AND ETO_ATR_HIST_OLD_VALUE IS NULL
AND ETO_ATR_COLUMN_VALUE NOT LIKE '%Quantity Unit%' AND 
ETO_ATR_COLUMN_VALUE NOT LIKE '%Currency%'
AND TRUNC(ETO_ATR_HIST_UPD_DATE) >= to_date(TRUNC(SYSDATE)- to_char(sysdate, 'd'))
GROUP BY FK_ETO_OFR_DISPLAY_ID
)
select SPEC_BUCKET, REGULAR_FILLED/AVAIL_QUES "ISQFR_directbl_organic",
REGULAR_FILLED/LEADS_GEN as "ISQfilled_BL",
LEADS_GEN,
LEADS_GEN_WITH_ISQ,
LEADS_WITH_FILLED_ISQ,
AVAIL_QUES,
AVAIL_QUES_CUST,
FILLED_RESPONSES,
CUSTOM_FILLED,
REGULAR_FILLED 
from (
SELECT  SPEC_BUCKET,
COUNT(DISTINCT ETO_OFR_DISPLAY_ID) LEADS_GEN,
COUNT(DISTINCT DECODE(GLCAT_MCAT_ID,NULL,NULL,ETO_OFR_DISPLAY_ID)) LEADS_GEN_WITH_ISQ,
COUNT(DISTINCT DECODE(GEN_MCAT,-1,NULL,NULL,NULL,DECODE(FILLED_RESPONSES,NULL,NULL,ETO_OFR_DISPLAY_ID))) LEADS_WITH_FILLED_ISQ,
SUM(NUM_QUESTIONS) AVAIL_QUES,
SUM(NUM_QUESTIONS) + SUM(DECODE(GEN_MCAT,-1,0,NULL,0,CUSTOM_FILLED)) AVAIL_QUES_CUST,
SUM(DECODE(GEN_MCAT,-1,0,NULL,0,FILLED_RESPONSES)) FILLED_RESPONSES,
SUM(DECODE(GEN_MCAT,-1,0,NULL,0,CUSTOM_FILLED)) CUSTOM_FILLED,
SUM(DECODE(GEN_MCAT,-1,0,NULL,0,REGULAR_FILLED)) REGULAR_FILLED
FROM MY_OFFERS,    (
SELECT CASE WHEN BUYER_SIDE_ISQ IS NULL THEN '0 SPECS'
   WHEN BUYER_SIDE_ISQ IN (1) THEN '1 SPEC'
 WHEN BUYER_SIDE_ISQ IN (2) THEN '2 SPECS'
  WHEN BUYER_SIDE_ISQ IN (3) THEN '3 SPECS'
   WHEN BUYER_SIDE_ISQ IN (4) THEN '4 SPECS'
    WHEN BUYER_SIDE_ISQ IN (5) THEN '5 SPECS'
    ELSE '>5 SPECS' END AS SPEC_BUCKET ,IM_CAT_SPEC_CATEGORY_ID
    FROM 
    (SELECT IM_CAT_SPEC_CATEGORY_ID,
    COUNT(1) BUYER_SIDE_ISQ
    FROM IM_SPECIFICATION_MASTER ,
    IM_CAT_SPECIFICATION
    WHERE IM_SPEC_MASTER_ID = FK_IM_SPEC_MASTER_ID
    AND IM_CAT_SPEC_CATEGORY_TYPE = 3
    AND IM_SPEC_MASTER_DESC NOT     IN ('Quantity Unit','Currency') 
    AND IM_SPEC_MASTER_BUYER_SELLER <> 2
    AND IM_CAT_SPEC_STATUS = 1
    GROUP BY IM_CAT_SPEC_CATEGORY_ID
    )),
MY_GEN_MCAT,
MY_ATTRIBUTES,
MY_FILLED_RESPS
WHERE ETO_OFR_DISPLAY_ID = FK_ETO_OFR_ID(+)
AND GEN_MCAT = GLCAT_MCAT_ID(+)
AND ETO_OFR_DISPLAY_ID = RESP_DISPLAY_ID(+)
AND IM_CAT_SPEC_CATEGORY_ID = GLCAT_MCAT_ID
GROUP BY ROLLUP (SPEC_BUCKET))


            """ 

df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates = pd.read_sql(querystring, con=conn)

df_final.abc[181] = df_final.abc[101]
df_final.abc[182]=df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[0] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[0]
df_final.abc[183]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[0] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[0])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.AVAIL_QUES[0] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_AVAIL[0])
df_final.abc[184]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[0] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[0])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[0] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[0])
df_final.abc[185] = df_final.abc[102]
df_final.abc[186]=df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[1] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[1]
df_final.abc[187]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[1] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[1])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.AVAIL_QUES[1] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_AVAIL[1])
df_final.abc[188]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[1] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[1])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[1] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[1])
df_final.abc[189] = df_final.abc[103]
df_final.abc[190]=df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[2] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[2]
df_final.abc[191]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[2] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[2])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.AVAIL_QUES[2] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_AVAIL[2])
df_final.abc[192]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[2] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[2])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[2] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[2])
df_final.abc[193] = df_final.abc[104]
df_final.abc[194]=df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[3] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[3]
df_final.abc[195]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[3] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[3])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.AVAIL_QUES[3] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_AVAIL[3])
df_final.abc[196]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[3] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[3])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[3] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[3])
df_final.abc[197] = df_final.abc[105]
df_final.abc[198]=df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[4] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[4]
df_final.abc[199]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[4] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[4])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.AVAIL_QUES[4] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_AVAIL[4])
df_final.abc[200]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[4] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[4])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[4] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[4])
df_final.abc[201] = df_final.abc[106]
df_final.abc[202]=df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[5] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[5]
df_final.abc[203]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[5] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[5])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.AVAIL_QUES[5] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_AVAIL[5])
df_final.abc[204]=(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.REGULAR_FILLED[5] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.GEN_ISQ_QUESTIONS_FILLED_USER[5])/(df_SPEC_BUCKET_WISE_Organic_Inorganic_BL_fill_rates.LEADS_GEN[5] + df_SPEC_BUCKET_WISE_Organic_Inorganic_FENQ_fill_rates.TOTAL_GENERATED[5])
df_final.abc[205] = df_final.abc[107]

#-----------------------Specs Available Over MCATs-------------------------------
