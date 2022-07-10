# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 15:40:54 2018

@author: Vaibhav
"""
#-------------------------About The Code--------------------------
#------------------------The code is to scrap the data from https://tyres.cardekho.com/ website------------------------

from lxml import html  
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import time as t
import pandas as pd
from selenium.webdriver.common.by import By

#Car_Tyres_Code
car_url  = 'https://tyres.cardekho.com/car-tyre-brands'
response_car = requests.get(car_url)
if response_car.status_code != 200:
        print('Failed to retrieve articles with error {}'.format(response_car.status_code))
        exit()
#Bike_Tyres_Code
bike_url='https://tyres.cardekho.com/bike-tyre-brands'
response_bike = requests.get(bike_url)
if response_bike.status_code != 200:
        print('Failed to retrieve articles with error {}'.format(response_bike.status_code))
        exit()
#Truck_Tyres_Code
truck_url='https://tyres.cardekho.com/truck-tyre-brands'
response_truck = requests.get(truck_url)
if response_truck.status_code != 200:
        print('Failed to retrieve articles with error {}'.format(response_truck.status_code))
        exit()
df=pd.DataFrame(columns={'URL','Product Name','Product Image','Product Price','Feature','Specification','Specification Value' })  
df1=pd.DataFrame(columns={'Product Page URL','Tyre Type'})      

brand_url= []
soupvar_car = BeautifulSoup(response_car.content, "html.parser")
soupvar_bike = BeautifulSoup(response_bike.content, "html.parser")
soupvar_truck = BeautifulSoup(response_truck.content, "html.parser")


link_car = soupvar_car.find_all('li', attrs={'class': 'tyrebrand'})
for li in link_car:
    car = li.find_all('a') 
    for car_value in range(0,len(car)):
       next_car_url = car[car_value].get('href')
       brand_url.append(next_car_url)
a=len(brand_url)

link_bike = soupvar_bike.find_all('li', attrs={'class': 'tyrebrand'})
for li in link_bike:
    bike = li.find_all('a')
    for bike_value in range(0,len(bike)):
       next_bike_url = bike[bike_value].get('href')
       brand_url.append(next_bike_url)
b=len(brand_url)    
  
link_truck = soupvar_truck.find_all('li', attrs={'class': 'tyrebrand'})
for li in link_truck:
    truck = li.find_all('a')
    for truck_value in range(0,len(truck)):
       next_truck_url = truck[truck_value].get('href')
       brand_url.append(next_truck_url)   
c=len(brand_url) 
    
#prefs = {'profile':{}} # 1=Allow popups, 2=Block popups 
#prefs['profile']['default_content_setting_values']={"popups":1}   
options = webdriver.ChromeOptions()
options.add_argument('--ignore-certificate-errors')
options.add_argument("--start-maximized")
options.add_argument("--test-type")
#options.add_experimental_option("prefs",prefs) 
#options.add_argument("--headless")
next_page_url=[]
next_page=[]
next_link =[]
i=32
k=0
for i in range(31,len(brand_url)):
    
    brand_url[i]='https://tyres.cardekho.com/aeolus'
    #brand_url[i]='https://tyres.cardekho.com/birla'
    #brand_url[i]='https://tyres.cardekho.com/aramis'

    driver = webdriver.Chrome(executable_path='C:/Users/Rasika/Desktop/Python_code/chromedriver.exe',chrome_options=options)
    driver.get(brand_url[i]) 
    t.sleep(5)
    
    
    #driver = webdriver.Firefox(executable_path='C:/Users/imart/Downloads/geckodriver-v0.21.0-win64/geckodriver.exe')
    container = driver.find_element_by_id("connecto_58e353880b1cbe9c21548969")
    driver.execute_script("arguments[0].style.display = 'none';", container)
    t.sleep(5)
    driver.find_element(By.XPATH,'//*[@id="searchLocationModal"]/a').click()
    #driver.execute_script(" window.addEventListener('load', function(){document.getElementById('connecto_58e353880b1cbe9c21548969').innerHTML=' ';});")
    t.sleep(5)
    if(i >=0 and i<a):
        #continue
        tyre_type='Car Tyre'
        driver.execute_script("window.document.getElementsByClassName('vehicle_type_radio ')[0].click()")
        #driver.find_element(By.XPATH,'//*[@id="versionCtrl"]/div/div/div[1]/div/div[1]/div/label[1]').click()
    elif(i>=a and i<b):
        tyre_type='Bike Tyre'
        if(driver.find_element(By.XPATH,'//*[@id="versionCtrl"]/div/div/div[1]/div/div[1]/div/label[1]').get_attribute('data-value')=='Car'):
            driver.find_element(By.XPATH,'//*[@id="versionCtrl"]/div/div/div[1]/div/div[1]/div/label[2]').click()
        else:
            driver.find_element(By.XPATH,'//*[@id="versionCtrl"]/div/div/div[1]/div/div[1]/div/label[1]').click()
    else:
        tyre_type='Truck Tyre'
        if(driver.find_element(By.XPATH,'//*[@id="versionCtrl"]/div/div/div[1]/div/div[1]/div/label[1]').get_attribute('data-value')=='Car'):
            if(driver.find_element(By.XPATH,'//*[@id="versionCtrl"]/div/div/div[1]/div/div[1]/div/label[2]').get_attribute('data-value')=='Bike'):
                driver.find_element(By.XPATH,'//*[@id="versionCtrl"]/div/div/div[1]/div/div[1]/div/label[3]').click()
            else:
                driver.find_element(By.XPATH,'//*[@id="versionCtrl"]/div/div/div[1]/div/div[1]/div/label[2]').click()  
        elif(driver.find_element(By.XPATH,'//*[@id="versionCtrl"]/div/div/div[1]/div/div[1]/div/label[1]').get_attribute('data-value')=='Bike'):
                driver.find_element(By.XPATH,'//*[@id="versionCtrl"]/div/div/div[1]/div/div[1]/div/label[2]').click()
        else:
            driver.find_element(By.XPATH,'//*[@id="versionCtrl"]/div/div/div[1]/div/div[1]/div/label[1]').click()
            
        
    t.sleep(5)

    
    #eg.click()
    lenOfPage = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
       
    match=False
    while(match==False):
        lastCount = lenOfPage
        driver.execute_script("window.document.getElementById('shw_more_res').click()")
        t.sleep(5)
        lenOfPage = driver.execute_script("window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
  #document.getElementsByClassName('viewmorebtn-inner').addEventListener('click', function(){})          
        if lastCount==lenOfPage:
                match=True        
                
    source_data = driver.page_source
    
#Brand_Page Get all the link   
    
    soup2 = BeautifulSoup(source_data, "html.parser")
    next_link = soup2.find_all('div', attrs={'class': 'link-button'})
    for specs in range(0,len(next_link)):
       next_page = next_link[specs].find('a').get('href')
       df1.set_value(k,'Product Page URL',next_page)
       next_page_url.append(next_page)
       
       df1.set_value(k,'Tyre Type',tyre_type)
       k+=1
      
specs=0
j=0 
df1.to_csv('C:/Users/Rasika/Desktop/Python_code/Product_Page_URL.csv', index=False) 

new_df=pd.read_excel('C:/Users/Rasika/Desktop/Tyre_Dekho/Tyre_Data.xlsx')
my_data=new_df.Link
my_list=  list(my_data)    
j=0
for url in my_list[0:]:



           #url= 'https://tyres.cardekho.com/mrf/ztx/145-80-r12-74s'
           headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36'}
           
           page = requests.get(url,headers = headers,verify=False)
           page_response = page.text
           parser = html.fromstring(page.content)
           t.sleep(5)
           if page.status_code != 200:
   
				   df.set_value(j,'Product Name','Page Not Found')
				   df.set_value(j,'Product Image','Page Not Found')
				   df.set_value(j,'Product Price','Page Not Found')
				   df.set_value(j,'Feature','Page Not Found')
				   df.set_value(j,'Specification','Page Not Found')
				   df.set_value(j,'Specification Value','Page Not Found')
				   df.set_value(j,'URL',url)
				   j+=1

           else:
				   productname = parser.xpath('//*[@id="pagecontent"]/div[3]/div/div/div[1]/div[1]/div[2]/h1')
				   name = productname[0].text_content().strip()
				   
				   
				   productspecs = BeautifulSoup(page.content, "html.parser")
				   imglink = productspecs.find('img', attrs={'class': 'drift-demo-trigger'})
				   img = imglink.get('data-src')
				   productprice = parser.xpath('//*[@id="pagecontent"]/div[3]/div/div/div[2]/div[1]/div/span')
				   price = productprice[0].text_content().strip()
				   
				   productfeature = productspecs.find('div', attrs={'id': 'fullEditorialReview'})
				   productfeaturevalue = productfeature.text.strip().split('.\n')
				  
				   
				   spec_head =[]
				   producthead = productspecs.find_all('span', attrs={'class': 'vrsnhead'})
				   for specs_1 in range(0,len(producthead)):
					   spec_head.append(producthead[specs_1].text)
					   
				   spec_value=[]
				   productvalue= productspecs.find_all('div', attrs={'class': 'vrsnbtn'})
				   for specs_2 in range(0,len(productvalue)):
					   spec_value.append(productvalue[specs_2].text)
					   
				   
				   product_spec= []
				   productspecsdetail=productspecs.find_all('p', attrs={'class': 'specfication-content'})
				   for specs_3 in range(0,len(productspecsdetail)):
					   product_spec.append(productspecsdetail[specs_3].text) 
					
					
					
				   df.set_value(j,'Product Name',name)
				   df.set_value(j,'Product Image',img)
				   df.set_value(j,'Product Price',price)
				   df.set_value(j,'Feature',productfeaturevalue)
				   df.set_value(j,'Specification',spec_head)
				   df.set_value(j,'Specification Value',spec_value)
				   df.set_value(j,'URL',url)
				   print(j)
				   j+=1
           
df.to_csv('C:/Users/Rasika/Desktop/Tyre_Dekho/Tyre_Dekho.csv', index=False)


df_m=df

final_df=pd.concat([new_df,df_m], ignore_index=True, axis =1)

final_df.to_csv('C:/Users/Rasika/Desktop/Tyre_Dekho/Tyre_Dekho_final_scrapping_after_concat.csv', index=False)

final_merge=pd.merge([new_df,df_m], left_on='Link',right_on='URL', how= 'Outer')
df.to_csv('C:/Users/Rasika/Desktop/Tyre_Dekho/Tyre_Dekho_final_scrapping.csv', index=False)
           

final_df=pd.concat([df1,df], ignore_index=True, axis =1)
final_merge=pd.merge([df1,df], left_on='link',right_on='URL' )
df.to_csv('C:/Users/Rasika/Desktop/Tyre_Dekho/Tyre_Dekho_final_scrapping.csv', index=False)

#----------------------Code to get Comaptible with, All Extra Images and Warranty--------------------------
from lxml import html  
import requests
from bs4 import BeautifulSoup
#from selenium import webdriver
import time as t
import pandas as pd
#from selenium.webdriver.common.by import By

df=pd.read_excel("C:/Users/imart/Desktop/Tyre_Dekho_Scrapping_part_2/Tyredekho data-Bike tyres.xlsx")
df.columns
df.drop(['Standard/Variant', 'Enhanced Name',
       'MCAT mapping', 'Unnamed: 17', 'Unnamed: 18', 'Unnamed: 19',
       'Unnamed: 20', 'Unnamed: 21', 'Unnamed: 22', 'Unnamed: 23',
       'Unnamed: 24', 'Unnamed: 25', 'Unnamed: 26', 'Unnamed: 27'], axis=1, inplace=True)
j=0
df.drop(['Video', 'PDF'], axis=1, inplace=True)
compatible=[]
image_data=[]
add_data_1=[]
add_data=''
str3=''
next_page_url=df["Reference"]
for url in next_page_url[0:]:
           #url= ' https://tyres.cardekho.com/mrf/rib/2-75-172'
           headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36'}
           
           page = requests.get(url,headers = headers,verify=False)
           page_response = page.text
           parser = html.fromstring(page.content)
           t.sleep(5)
           productspecs = BeautifulSoup(page.content, "html.parser")
           if(page.status_code != 200):
               df.set_value(j,'Comaptible_With','Page Not Found')
               df.set_value(j,'Spec','Page Not Found')
               df.set_value(j,'Spec_Value','Page Not Found')
               df.set_value(j,'Extra_Image','Page Not Found')
               
               j+=1
           else:
                #productcompatible= productspecs.find('div', attrs={'id': 'compatible'})
                #productfeaturevalue = productcompatible.text.strip()
                #----Comaptible With-----------------
                productcompatible = productspecs.find_all('div', attrs={'id': 'compatible'})
                for li in productcompatible:
                    productcompatiblewith = li.find_all('a')
                for value in range(0,len(productcompatiblewith)):
                    next_data = productcompatiblewith[value].get('title')
                    next_data=next_data.split(" Tyres")
                    next_data=next_data[0]
                    compatible.append(next_data)
                str1 = ','.join(compatible)
                df.set_value(j,'Comaptible_With',str1)
                str1=''
                next_data=''
                compatible=[]

                #-----------------Specification------------------------
                war = productspecs.find_all('div', attrs={'class': 'version-sepcs'}) 
                for li_all in war:
                    war_det = li_all.find_all('label')
               
                for value_war in range(0,len(war_det)-1):
                    add_data = war_det[value_war].text
                    add_data_1.append(add_data)
                str3 = ','.join(add_data_1)
                df.set_value(j,'Spec',str3)
                add_data_1=[]
                add_data=''
                str3=''
                   
                for div_val in war:
                    war_val = div_val.find_all('div')
               
                for value in range(0,len(war_det)-1):
                    add_data = war_val[value].text
                    add_data_1.append(add_data)
                str3 = ','.join(add_data_1)
                df.set_value(j,'Spec_Value',str3)
                add_data_1=[]
                add_data=''
                str3=''
                 #--------------------------------Image-------------------------------------  
                try:
                     imglink = productspecs.find_all('div', attrs={'class': 'img-small-gallery'})
                     for img_value in range(1,len(imglink)):
                            next_img = imglink[img_value].find('img').get('data-src')
                            next_img=next_img.replace('114x75',"360x234")
                            image_data.append(next_img)
                     str2 = ','.join(image_data)
                     df.set_value(j,'Extra_Image',str2)
                except:
                   df.set_value(j,'Extra_Image',"Image not Present")
                next_img=''
                image_data=[]
                str2=''
                   
                j+=1
                print(j)
            
            
df.to_csv("C:/Users/imart/Desktop/Tyre_Dekho_Scrapping_part_2/Tyredekho-Bike_tyres_Data.csv")

#-----------------------------------------Code to get Breadcrumb and Tubeless Options---------------------------------



from lxml import html  
import requests
from bs4 import BeautifulSoup
#from selenium import webdriver
import time as t
import pandas as pd
#from selenium.webdriver.common.by import By

#k=df["Reference"].unique()

df=pd.read_csv("C:/Users/Rasika/Desktop/Tyre_dekho_slot_3/Tyre_Dekho.csv")
#df=df.head(5)
df.columns
breadcrumb=df["URL"]
j=0
for url in breadcrumb[0:]:
           #url= ' https://tyres.cardekho.com/mrf/zapper-fg/90-90-12'
           headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36'}
           
           page = requests.get(url,headers = headers,verify=False)
           page_response = page.text
           parser = html.fromstring(page.content)
           t.sleep(5)
           productspecs = BeautifulSoup(page.content, "html.parser")
           if(page.status_code != 200):
               df.set_value(j,'Tyre_Type','Page Not Found')
               df.set_value(j,'Breadcrumb','Page Not Found')
               j+=1
           else:
                #productcompatible= productspecs.find('div', attrs={'id': 'compatible'})
                #productfeaturevalue = productcompatible.text.strip()
                #----Comaptible With-----------------
              try:
                  tyre_type = parser.xpath('//*[@id="pagecontent"]/div[3]/div/div/div[1]/div[1]/div[2]/h2')
                  tyre_data=tyre_type[0].text_content().strip()
                  
              except:
                  
                  tyre_data="Page Not available"
                  
              df.set_value(j,'Tyre_Type',tyre_data)
              
              breadcrumb_data=[]     
              data = productspecs.find_all('a', attrs={'id': 'oemLink'})
              for specs in range(0,len(data)):
                       data_1 = data[specs].get('title')
                       breadcrumb_data.append(data_1)
              breadcrumb_data=str(breadcrumb_data)
              df.set_value(j,'Breadcrumb',breadcrumb_data)
              j+=1
              print(j)
    
df.to_csv("C:/Users/Rasika/Desktop/Tyre_dekho_slot_3/Tyre_Dekho_output.csv")
           
           
            