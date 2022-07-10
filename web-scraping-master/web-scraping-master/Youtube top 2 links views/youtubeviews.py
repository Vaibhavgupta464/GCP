#Importing libraries
from selenium import webdriver
import pyautogui
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
height = 0


#Reading the input file
df = pd.read_excel('C:/Users/Imart/Desktop/Keyword extraction doon/Input/Desktop 1.xlsx')

#Converting the MCAT names into a list
listofmcats = df['MCAT'].tolist()
#listofmcats = listofmcats[1213:]
#The final structure of output data required
finaldf = pd.DataFrame(columns = ['Keyword','Publisher','VideoURL','Title','Description','Number of views','Date of Publishing','Length of videos'])

row = 0

for mcat in listofmcats:
    
    
    #Replacing some characters as per suitability to the url encryption
    keyword = mcat.replace(' ','%20')
    keyword = keyword.replace('&','%26')
    
    
    #Opening the browser window
    driver = webdriver.Chrome(executable_path='C:/Users/Imart/Downloads/chromedriver.exe',chrome_options=options)
    
    count=0
    #Increasing the height of mouse coordinates in order to click the top two result links(if any)
    for height in range(0,621,155):    
            #Go to the youtube page
        driver.get('https://www.youtube.com/results?search_query='+keyword)
        
        
        t.sleep(5)
        
        
        #Handles exception if no results are found for the input keywords
        try:
            noresults = driver.find_element_by_class_name('promo-title')
            title = 'No results found'
            description= 'No results found'
            views = 'No results found'
            publisher = 'No publisher found'
            date= 'No publishing date found'
            length = 'Length not found'
            link = driver.current_url
            finaldf.set_value(row,'Keyword',mcat)
            finaldf.set_value(row,'Publisher',publisher)
            finaldf.set_value(row,'VideoURL',link)
            finaldf.set_value(row,'Title',title)
            finaldf.set_value(row,'Description',description)
            finaldf.set_value(row,'Number of views',views)
            finaldf.set_value(row,'Date of Publishing',date)
            finaldf.set_value(row,'Length of videos',length)
            row = row + 1
            
            break
        except:
            print("Results found")
        
        
        #Finding and setting the duration of the video (if found)
        try:
            length = driver.find_elements_by_class_name('ytd-thumbnail-overlay-time-status-renderer')[count].text
            count+=1
        except:
            length = "Length not found"
        
        #Clicking  on the result link by moving cursor coordinates
        pyautogui.size()
        pyautogui.position()
        curWindowHndl = driver.current_window_handle
        
        pyautogui.keyDown('ctrl')
        pyautogui.keyDown('shift')
        
        pyautogui.click(x=625, y=360+height,button='left')
        pyautogui.keyUp('shift')
        pyautogui.keyUp('ctrl')
        
        
        #Handle exception if there is a single video result for a keyword
        try:
            driver.switch_to.window(driver.window_handles[1])
        except:
            print("Single Video")
            continue
        t.sleep(6)
        
        #Handles exception for no publisher available
        try:
                publisher=driver.find_element_by_xpath('//*[@id="owner-name"]/a').text
                
                
        except:
            publisher = "No publisher available"
        
        
        #Handles exception for no title available
        try:
                title = driver.find_elements_by_xpath('//*[@id="container"]/h1/yt-formatted-string')[0].text
                
                
        except:
            title = "No title available"
        
        
        #Handles exception for no description available
        try:
            
            description = driver.find_elements_by_xpath('//*[@id="description"]/yt-formatted-string')[0].text
            
        except:
            
            description = "No description available"
        
        
        #Handles exception for no view count available
        try:
            views = driver.find_element_by_class_name('view-count').text
        except:
            views = "Views Not available"
            
        
        #Setting the video date of publication
        try:
            date = driver.find_element_by_class_name("date").text
            
        except:
            date =  "No video publish date found"
        
        
        finally:
            
            
            #Setting the final values into the output dataframe
            link = driver.current_url
            finaldf.set_value(row,'Keyword',mcat)
            finaldf.set_value(row,'Publisher',publisher)
            finaldf.set_value(row,'VideoURL',link)
            finaldf.set_value(row,'Title',title)
            finaldf.set_value(row,'Description',description)
            finaldf.set_value(row,'Number of views',views)
            finaldf.set_value(row,'Date of Publishing',date)
            finaldf.set_value(row,'Length of videos',length)
            row = row + 1
        driver.close()
        driver.switch_to.window(curWindowHndl)
    driver.quit()


#Saving the output to a dataframe    
finaldf.to_csv("YoutubeVideosTitleDescriptionViews1.csv")