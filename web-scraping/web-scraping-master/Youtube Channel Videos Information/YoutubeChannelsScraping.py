# -*- coding: utf-8 -*-
"""
Created on Thu Jan  3 14:37:52 2019

@author: Vaibhav
"""

#Importing libraries
from selenium import webdriver
#import pyautogui
import time as t
import pandas as pd


'''  For Google Chrome browser'''
options = webdriver.ChromeOptions()

''' For Firefox browser'''
#options = webdriver.FirefoxOptions()


#Adding required options for opening the browser
options.add_argument('--ignore-certificate-errors')
options.add_argument("--test-type")
options.add_argument("--start-maximized")
#options.add_argument("--headless")
#height = 0

'''
#Reading the input file
'''
df = pd.read_excel('C:/Users/prachi/Desktop/My_Data/Youtube Videos views/channellinks.xlsx')

#Converting the MCAT names into a list
listofchannels = df['Video'].tolist()


#The final structure of output data required
finaldf = pd.DataFrame(columns = ['Channel','VideoURL','Title','Description','Duration','Publishing Date','Number of views','Likes','Dislikes','Owner'])

row = 0


#Iterating over the channels one by one
for channel in listofchannels:
    
    
    
    #Opening the browser window
    driver = webdriver.Chrome(executable_path='C:/Users/prachi/Downloads/chromedriver.exe',chrome_options=options)
    
    #Go to the videos page in the channel
    driver.get(channel+'/videos')
    
    t.sleep(5)
    
    #Finding the channel name
    channelname = driver.find_element_by_id('channel-title').text
    
    
    
    
    # Selenium script to scroll to the bottom, wait 3 seconds for the next batch of data to load, then continue scrolling.  It will continue to do this until the page stops loading new data.
    
    
    lenOfPage = driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);var lenOfPage=document.documentElement.scrollHeight;return lenOfPage;")
    match=False
    while(match==False):
            lastCount = lenOfPage
            t.sleep(5)
            lenOfPage = driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);var lenOfPage=document.documentElement.scrollHeight;return lenOfPage;")
            if lastCount==lenOfPage:
                match=True
    
    
    #Get the links of all videos in the channel
    linkelements = driver.find_elements_by_id('thumbnail')
    
    
    videourls = [ v.get_attribute('href') for v in linkelements]
    
    
    #Get the video duration of the videos
    try:
        
        videoduration = [v.text for v in driver.find_elements_by_css_selector(".style-scope.ytd-thumbnail-overlay-time-status-renderer")]
            
    except:
        videoduration =  "No video duration found"
    
    
    #Closing te browser
    driver.quit()


    #Iterating over the videos in the channel
    for i in range(0,len(videourls)):
    
        
        #The links given in the input
        driver = webdriver.Chrome(executable_path='C:/Users/prachi/Downloads/chromedriver.exe',chrome_options=options)
        
        
        #Opening the video link
        driver.get(videourls[i])
        
        t.sleep(5)
        
        #Setting the video date of publication
        try:
            date = driver.find_element_by_class_name("date").text
            
        except:
            date =  "No video publish date found"
        
        #Setting the video duration
        try:
            duration = videoduration[i]
            
        except:
            duration =  "No video duration found"
    
        
        #Handle exception when there is no title found in the url    
        try:
            title = driver.find_elements_by_xpath('//*[@id="container"]/h1/yt-formatted-string')[0].text
            
            
        except:
            title = "No title available"
        
        
        #Handle exception when there is no description found in the url      
        try:
            
            description = driver.find_elements_by_xpath('//*[@id="description"]/yt-formatted-string')[0].text
            
        except:
            
            description = "No description available"
            
        #Handle exception when there is no view count found in the url      
        try:
            
            views = driver.find_elements_by_xpath('//*[@id="count"]/yt-view-count-renderer/span[1]')[0].text
            
        except:
            
            views = "No view count available"
            
        #Finding the like count on a video
        try:
            like=[]
            likes = driver.find_elements_by_tag_name('yt-formatted-string')
            for l in range(0,len(likes)):
                if (likes[l].get_attribute('id')=='text' and likes[l].get_attribute('class')=='style-scope ytd-toggle-button-renderer style-text'):
                    like.append(likes[l].text)
            likes=like[0]
        except:
            
            likes = "No likes available"
            
        
        #Finding the dislike count on a window
        try:
            dislike=[]
            dislikes = driver.find_elements_by_tag_name('yt-formatted-string')
            for l in range(0,len(dislikes)):
                if (dislikes[l].get_attribute('id')=='text' and dislikes[l].get_attribute('class')=='style-scope ytd-toggle-button-renderer style-text'):
                    dislike.append(dislikes[l].text)
            dislikes= dislike[1]     
            
        except:
            
            dislikes = "No likes available"
            
            
        #Finding the owner of a video
        try:
            
            owner = driver.find_element_by_id('owner-container').text
            
        except:
            
            owner = "No owner available"
        
        
        #In either case, setting the values scraped into the final dataframe    
        finally:
            finaldf.set_value(row,'Channel',channel)
            finaldf.set_value(row,'VideoURL',videourls[i])
            finaldf.set_value(row,'Title',title)
            finaldf.set_value(row,'Description',description)
            finaldf.set_value(row,'Publishing Date',date)
            finaldf.set_value(row,'Duration',duration)
            finaldf.set_value(row,'Number of views',views)
            finaldf.set_value(row,'Likes',likes)
            finaldf.set_value(row,'Dislikes',dislikes)
            finaldf.set_value(row,'Owner',owner)
            row = row + 1
        
            t.sleep(5)
        
        driver.quit()



#Saving the output to a dataframe    
finaldf.to_csv("YoutubeChannelVideosTitleDescriptionViews.csv")
