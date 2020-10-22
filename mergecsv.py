import pandas as pd
import glob
# path= r'/var/rel8ed.to/nfs/share/buildwith/newcsv'
all_files=glob.glob("*.csv")

li=[]

for file in all_files:
    df=pd.read_csv(file,dtype='str',index_col=None,header=0,low_memory=False)
    li.append(df)
    print(file)


frame= pd.concat(li,axis=0,ignore_index=True)

frame.to_csv('foursquare20-21.csv',index=False,encoding='utf-8-sig')

#todo
