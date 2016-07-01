from celery.task import task
import pandas as pd
from datetime import datetime
from ftplib import FTP
import urllib, shutil
import sys
sys.path.append('/code/task_config')
from config import ftp_username, ftp_password
@task()
def teco_spruce_pulldata(destination='/data/local/spruce_data'):
    initial_text=open("{0}/initial.txt".format(destination),"r")
    #pulling data from the url
    
    url = 'ftp://{0}:{1}@sprucedata.ornl.gov/DataFiles/EM1_Table1.dat'.format(ftp_username,ftp_password)
    sp_data=pd.read_csv(url,skiprows=5)
    #print (sp_data)
    columnnames = ["TIMESTAMP","RECORD","Tair","RH","AirTCHumm_Avg","RH_Humm_Avg","VPD","Rain","WS","WindDir_D1_WVT","WindDir_SD1_WVT","WSDiag_Tot","SmplsF_Tot","Axis1Failed_Tot","Axis2Failed_Tot","BothAxisFailed_Tot","NVMerror_Tot","ROMerror_Tot","MaxGain_Tot","NNDF_Tot","HollowSurf_Avg","Hollow5cm_Avg","Tsoil","Hollow40cm_Avg","Hollow80cm_Avg","Hollow160cm_Avg","Hollow200cm_Avg","HummockSurf_Avg","Hummock5cm_Avg","Hummock20cm_Avg","Hummock40cm_Avg","Hummock80cm_Avg","Hummock160cm_Avg","Hummock200cm_Avg","PAR","PAR_NTree1_Avg","PAR_NTree2_Avg","PAR_SouthofHollow1_Avg","PAR_SouthofHollow2_Avg","PAR_NorthofHollow1_Avg","PAR_NorthofHollow2_Avg","PAR_Srub1_Avg","PAR_Srub2_Avg","PAR_Srub3_Avg","PAR_Srub4_Avg","TopofHummock_Avg","MidofHummock_Avg","Surface1_Avg","Surface2_Avg","D1-20cm_Avg","D2-20cm_Avg","TopH_Avg","MidH_Avg","S1_Avg","S2_Avg","Deep-20cm_Avg","short_up_Avg","short_dn_Avg","long_up_Avg","long_dn_Avg","CNR4_Temp_C_Avg","CNR4_Temp_K_Avg","long_up_corr_Avg","long_dn_corr_Avg","Rs_net_Avg","Rl_net_Avg","albedo_Avg","Rn_Avg","SPN1_Total_Avg","SPN1_Diffuse_Avg","Water_Height_Avg","Water_Temp_Avg","Watertable","Dewpoint","Dewpoint_Diff"]
       				
    sp_data.columns = columnnames
    
    #trying to bring the timestamp into a format
    df=sp_data
    data=df['TIMESTAMP']
    df['Date_Time']=pd.to_datetime(df['TIMESTAMP'])    
    
    #Trim columns
    teco_spruce =df[['Date_Time','Tair','Tsoil','RH','VPD','Rain','WS','PAR']]
    #adding it to the existing data frame
    df['year']=df['Date_Time'].dt.year
    df['doy']=df['Date_Time'].dt.dayofyear
    df['hour']=df['Date_Time'].dt.hour

    teco_spruce=df[['year','doy','hour','Tair','Tsoil','RH','VPD','Rain','WS','PAR']]
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
    teco_spruce1=teco_spruce.iloc[::2]
    #need to reset the index number[from 0 2 4 8] [to 0 1 2 3]
    teco_spruce1=teco_spruce1.reset_index(drop=True)
    #setting the mean of 'Tair','Tsoil','RH','VPD','WS','PAR' n sum of ,'Rain' to this new dataframe teco_spruce1
    teco_spruce1['Tair']=tair.reset_index(drop=True)	    
    teco_spruce1['Tsoil']=tsoil.reset_index(drop=True)
    teco_spruce1['RH']=rh.reset_index(drop=True)
    teco_spruce1['VPD']=vpd.reset_index(drop=True)
    teco_spruce1['Rain']=rain.reset_index(drop=True)
    teco_spruce1['WS']=ws.reset_index(drop=True)
    teco_spruce1['PAR']=par.reset_index(drop=True)
    
    #file which contain earlier data(2011-2015)
    j1=pd.read_csv(initial_text,'\t')
    
    #file which contain the new data
    #j2=pd.read_csv('teco_spruce.txt','\t')
    print "I found you############################################################3"
    #joining both the files together and removing the duplicate rows
    j3=pd.concat([j1,teco_spruce1]).drop_duplicates().reset_index(drop=True)
    
    #writing it to a file
    j3.to_csv('{0}/SPRUCE_forcing.txt'.format(destination),'\t',index=False)    

#teco_spruce_pulldata()
    
