import pandas as pd
from unidecode import unidecode
import os
from sqlalchemy import create_engine


filenames = os.listdir('csvfiles')
filenames = [f for f in filenames if f.endswith('.csv')]
e = create_engine('*****')
fail = open('failedsites.csv', 'a')
for filename in filenames:
    print(filename)
    try:
        df = pd.read_csv('csvfiles/'+filename, dtype='str',error_bad_lines=False)
        if df.columns[-1] == 'GDPR':
            df = df.rename(columns={df.columns[0]:'Domain'})
            df['filename'] = filename
            df.to_sql('buildwithData', e, if_exists='append', chunksize=1000)
        else:
            print('Bad format ', filename)
            fail.write(filename+',badformat\n')
    except Exception as e:
        print('Error!', filename, e)
        fail.write(filename+',loading failed\n')
