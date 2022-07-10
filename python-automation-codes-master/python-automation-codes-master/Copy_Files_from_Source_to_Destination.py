# -*- coding: utf-8 -*-
"""
Created on Fri Dec 28 13:33:22 2018

@author: Vaibhav
"""

#---------------------------About The Code-----------------------------
#To Copy some files from one location to another on the basis of some condition
import shutil
import pandas as pd

df=pd.read_excel("C:/Users/imart/Desktop/Test/MCATs_90%_products_mapped.xlsx",encoding = "ISO-8859-1")
i=0
#df=df.head(50)

#------#-----Copy Corpus data-------------
for i in range(0,len(df)):
    #if path.exists(str(df["MCAT_ID"][i])+".txt")
     try:
            shutil.copy("C:/Users/imart/Desktop/Corpus_data/Allcorpus/"+str(df["MCAT_ID"][i])+".txt", "C:/Users/imart/Desktop/new_corpus/Allcorpus/"+str(df["MCAT_ID"][i])+".txt")
     except:
         print("All_Corpus Not Found")
         
         
     try:
            shutil.copy("C:/Users/imart/Desktop/Corpus_data/major_corpus/"+str(df["MCAT_ID"][i])+".1.txt", "C:/Users/imart/Desktop/new_corpus/major_corpus/"+str(df["MCAT_ID"][i])+".1.txt")
     except:
         print("major_Corpus_1 Not Found")  
         
         
     try:
            shutil.copy("C:/Users/imart/Desktop/Corpus_data/major_corpus/"+str(df["MCAT_ID"][i])+".2.txt", "C:/Users/imart/Desktop/new_corpus/major_corpus/"+str(df["MCAT_ID"][i])+".2.txt")
     except:
         print("major_Corpus_2 Not Found")   
        
     try:
            shutil.copy("C:/Users/imart/Desktop/Corpus_data/major_corpus/"+str(df["MCAT_ID"][i])+".3.txt", "C:/Users/imart/Desktop/new_corpus/major_corpus/"+str(df["MCAT_ID"][i])+".3.txt")
    
     except:
         print("major_Corpus_3 Not Found")
     print(i)
	 
#-----Copy Input data-------------   
for i in range(0,len(df)):
    #if path.exists(str(df["MCAT_ID"][i])+".txt")
     try:
            shutil.copy("C:/Users/imart/Desktop/Testing_Output_file/"+str(df["MCAT_ID"][i])+".txt", "C:/Users/imart/Desktop/Input/"+str(df["MCAT_ID"][i])+".txt")
     except:
         print("Input_File Not Found")
     print(i)
