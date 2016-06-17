from celery.task import task
import pandas as pd
from datetime import datetime
from ftplib import FTP
import urllib, shutil
from config import ftp_username, ftp_password
@task()
def teco_spruce_pulldata():
  
    #pulling data from the url
    
    url = 'ftp://{0}:{1}@sprucedata.ornl.gov/DataFiles/EM1_Table1.dat'.format(ftp_username,ftp_password)
    sp_data=pd.read_csv(url,skiprows=5)
    #print (sp_data)
    columnnames = ["TIMESTAMP","RECORD","Tair","RH","AirTCHumm_Avg","RH_Humm_Avg","VPD","Rain","WS","WindDir_D1_WVT","WindDir_SD1_WVT","WSDiag_Tot","SmplsF_Tot","Axis1Failed_Tot","Axis2Failed_Tot","BothAxisFailed_Tot","NVMerror_Tot","ROMerror_Tot","MaxGain_Tot","NNDF_Tot","HollowSurf_Avg","Hollow5cm_Avg","Tsoil","Hollow40cm_Avg","Hollow80cm_Avg","Hollow160cm_Avg","Hollow200cm_Avg","HummockSurf_Avg","Hummock5cm_Avg","Hummock20cm_Avg","Hummock40cm_Avg","Hummock80cm_Avg","Hummock160cm_Avg","Hummock200cm_Avg","PAR","PAR_NTree1_Avg","PAR_NTree2_Avg","PAR_SouthofHollow1_Avg","PAR_SouthofHollow2_Avg","PAR_NorthofHollow1_Avg","PAR_NorthofHollow2_Avg","PAR_Srub1_Avg","PAR_Srub2_Avg","PAR_Srub3_Avg","PAR_Srub4_Avg","TopofHummock_Avg","MidofHummock_Avg","Surface1_Avg","Surface2_Avg","D1-20cm_Avg","D2-20cm_Avg","TopH_Avg","MidH_Avg","S1_Avg","S2_Avg","Deep-20cm_Avg","short_up_Avg","short_dn_Avg","long_up_Avg","long_dn_Avg","CNR4_Temp_C_Avg","CNR4_Temp_K_Avg","long_up_corr_Avg","long_dn_corr_Avg","Rs_net_Avg","Rl_net_Avg","albedo_Avg","Rn_Avg","SPN1_Total_Avg","SPN1_Diffuse_Avg","Water_Height_Avg","Water_Temp_Avg","Watertable","Dewpoint","Dewpoint_Diff"]
       				
    #write to a csv file

    #sp_data.to_csv('my1.csv',sep=';')
    #sp_data=pd.read_csv('my1.csv',skiprows=5)
    #sp_data.to_csv('my1.csv')
    sp_data.columns = columnnames
    
    #sp_data.to_csv('my1.csv')
    
    #df=pd.read_csv('my1.csv')
    #trying to bring the timestamp into a format
    df=sp_data
    data=df['TIMESTAMP']
    #data=data.str.lstrip('0123456789')
    #data=data.str.strip(';"')
    #adding it to the existing data frame
    df['Date_Time']=pd.to_datetime(df['TIMESTAMP'])    
    
    #Trim columns
    teco_spruce =df[['Date_Time','Tair','Tsoil','RH','VPD','Rain','WS','PAR']]
    #dividing Date_Time to year,day and hour
    #time=teco_spruce['Date_Time']
    #getting the year
    #year=time.str.split('-').str.get(0)
    #gettting the month
    #month=time.str.split('-').str.get(1)
    #getting the Day and hour
    #day_hour=time.str.split('-').str.get(2)
    #day_hour
    #doy=day_hour.str.split(' ').str.get(0)
    #hour=day_hour.str.split(' ').str.get(1)
    #getting the day number from the year,month and day
    #day=cal_day_num(year,month,day)
    #extracting only the hour from a given time
    #hour=hour.str.rstrip('0')
    #hour=hour.str.rstrip(':')
    #hour=hour.str.rstrip('1234567890')
    #hour=hour.str.rstrip(':')
    #adding it to the existing data frame
    df['year']=df['Date_Time'].dt.year
    df['doy']=df['Date_Time'].dt.dayofyear
    df['hour']=df['Date_Time'].dt.hour
    #df['month']=df['Date_Time'].dt.month
    #i=0
    #length=len(df.index)
    #for i in range(length):
     #   doy=df.iloc[i]['doy']
        #print('hello')
        #print('day->'+str(day))
        #print('hi')
      #  doy=int(doy)
       # month=df.iloc[i]['month']
        
        #month=int(month)
        #print('month->'+str(month))
        #year=df.iloc[i]['year']
        #year=int(year)
        #print('year->'+str(year))
        #my_date=datetime(2012,1,31)
        #my_date.timetuple().tm_yday
        #doy_cal=datetime(year,month,doy)
        #doy_cal=doy_cal.timetuple().tm_yday
        #print('day_cal->'+str(day_cal))
        #setting each day to day of year
        #df.loc[i,('doy')]=doy_cal
        #df.set_value('day',i,day_cal)
        #i=i+1
        #print('i->'+str(i))
        #df.iloc[i]['day']=day_cal 
    teco_spruce=df[['year','doy','hour','Tair','Tsoil','RH','VPD','Rain','WS','PAR']]
    #teco_spruce=pd.DataFrame(teco_spruce)
    #getting the mean of 'Tair','Tsoil','RH','VPD','WS','PAR' n sum of ,'Rain' by combining half n full hour(e.i.12 & 12:30)
    group_treat=teco_spruce.groupby(['year','doy','hour'])
    tair=group_treat['Tair'].mean()
    tsoil=group_treat['Tsoil'].mean()
    rh=group_treat['RH'].mean()
    vpd=group_treat['VPD'].mean()
    rain=group_treat['Rain'].sum()
    ws=group_treat['WS'].mean()
    par=group_treat['PAR'].mean()
    #Taking only the even coulums(as half hourly details not required) i.e. 12:30 not required only 12 required 
    #teco_spruce1=teco_spruce.iloc[::2]
    teco_spruce1=teco_spruce.iloc[::2]
    #need to reset the index number[from 0 2 4 8] [to 0 1 2 3]
    teco_spruce1=teco_spruce1.reset_index(drop=True)
    #i=0
    #length1=len(teco_spruce1.index)
    #setting the mean of 'Tair','Tsoil','RH','VPD','WS','PAR' n sum of ,'Rain' to this new dataframe teco_spruce1
    teco_spruce1['Tair']=tair.reset_index(drop=True)	    
    teco_spruce1['Tsoil']=tsoil.reset_index(drop=True)
    teco_spruce1['RH']=rh.reset_index(drop=True)
    teco_spruce1['VPD']=vpd.reset_index(drop=True)
    teco_spruce1['Rain']=rain.reset_index(drop=True)
    teco_spruce1['WS']=ws.reset_index(drop=True)
    teco_spruce1['PAR']=par.reset_index(drop=True)
  #  for i in range(length1):
   #     teco_spruce1.loc[i,('Tair')]=tair.iloc[i]
    #    teco_spruce1.loc[i,('Tsoil')]=tsoil.iloc[i]
     #   teco_spruce1.loc[i,('RH')]=rh.iloc[i]
      #  teco_spruce1.loc[i,('VPD')]=vpd.iloc[i]
       # teco_spruce1.loc[i,('Rain')]=rain.iloc[i]
        #teco_spruce1.loc[i,('WS')]=ws.iloc[i]
        #teco_spruce1.loc[i,('PAR')]=par.iloc[i]
       
    
    #Write to tab delimited file
    #teco_spruce1.to_csv('teco_spruce.txt','\t',index=False)
    # joining the new file with the earlier file
    
    #file which contain earlier data(2011-2015)
    j1=pd.read_csv('initial.txt','\t')
    
    #file which contain the new data
    #j2=pd.read_csv('teco_spruce.txt','\t')
    
    #joining both the files together and removing the duplicate rows
    j3=pd.concat([j1,teco_spruce1]).drop_duplicates().reset_index(drop=True)
    
    #writing it to a file
    j3.to_csv('final.txt','\t',index=False)    

#teco_spruce_pulldata()
    
