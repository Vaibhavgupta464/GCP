# -*- coding: utf-8 -*-
"""
Created on Thu Aug 30 10:17:10 2018

@author: vaibhav
"""
from selenium import webdriver
import time as t
import pandas as pd

#Reading the input file having video urls

df = pd.read_csv('C:/Users/IMART/Downloads/Zendesk videos to upload.csv')
listofurls1 = df['Video'].tolist()

''' Code to be used in case there are multiple video url columns'''

#listofurls2=df['Video 2'][df['Video 2'].notnull()].tolist()
#listofurls3=df['Video 3'][df['Video 3'].notnull()].tolist()

#totallistofurls = listofurls1 + listofurls2 + listofurls3

'''The structure of the final output required'''

finaldf = pd.DataFrame(columns = ['VideoURL','Title','Description'])


#Loading additional functionalities for running the web browser

'''  For Google Chrome browser'''
options = webdriver.ChromeOptions()

''' For Firefox browser'''
#options = webdriver.FirefoxOptions()

options.add_argument('--ignore-certificate-errors')
options.add_argument("--test-type")
options.add_argument("--headless")

row = 0 

for i in range(0,len(listofurls1)):
    
        
    #The links given in the input
    finallink = listofurls1[i] 
    
    #The final youtube link required to scrape out the titles and descriptions
    
    #finallink = "https://www.youtube.com/watch?v=4RvEwLJ0Z9&feature=youtu.be"
    #finallink = "https://www.youtube.com/watch?v=1z38xToPvdg&feature=youtu.be"
    #finallink = "https://www.youtube.com/watch?v=9GQfp8RV31A&feature=youtu.be"
    driver = webdriver.Chrome(executable_path='C:/Users/prachi/Downloads/chromedriver.exe',chrome_options=options)
    
    
    ''' For Firefox browser'''
    #webdriver.Firefox(executable_path='C:/Users/IMART/Downloads/geckodriver-v0.21.0-win64/geckodriver.exe')
    
    driver.get(finallink)
    
    t.sleep(5)
    
    
    #Handle exception when there is no title found in the url    
    try:
        title = driver.find_elements_by_xpath('//*[@id="container"]/h1/yt-formatted-string')[0].text
        
        
    except:
        title = "No video available"
    
    
    #Handle exception when there is no description found in the url      
    try:
        
        description = driver.find_elements_by_xpath('//*[@id="description"]/yt-formatted-string')[0].text
        
    except:
        
        description = "No video available"
        
    
    #In either case, setting the values scraped into the final dataframe    
    finally:
    
        finaldf.set_value(row,'VideoURL',listofurls1[i])
        finaldf.set_value(row,'Title',title)
        finaldf.set_value(row,'Description',description)
        row = row + 1
    
        t.sleep(5)
    
    driver.quit()
    

#Saving the final dataframe information into a csv file
finaldf.to_csv("C:/Users/IMART/Desktop/My_Data/ZendeskYoutubeVideosDescription.csv")
