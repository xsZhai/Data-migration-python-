import pandas as pd
from unidecode import unidecode

import os
# filenames = os.listdir('originFile')
# filenames = [f for f in filenames if f.endswith('.xlsx')]

# for filename in filenames:
#     print(filename)
#     df = pd.read_excel('originFile/'+filename, skiprows=1)
#     df.to_csv('csvfiles/'+filename.replace('.xlsx', '.csv'), index=0)
#     with open('csvfiles/'+filename.replace('.xlsx', '.csv'),'r+') as f:
#         text = f.read()
#         text0 = unidecode(text)
#         f.truncate(0)
#         f.write(text0)


# import findspark
# findspark.init()
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()


# df = spark.read.csv('/var/rel8ed.to/buldwith/csvfiles/*', header=True, escape='"')


# pdf = pd.read_csv('/var/rel8ed.to/buldwith/csvfiles/All-Live-Adobe-CQ-Sites.csv', dtype='str')
# pdf = pdf.rename(columns={pdf.columns[0]:'Domain'})

# from sqlalchemy import create_engine

# e = create_engine('mysql://root:rel8edto@localhost/buildwith')

# pdf.to_sql('buildwith0', e)

# conn = e.connect()
# rs = conn.execute('select * from buildwith0 limit 10')

# dfall = pd.read_csv('/var/rel8ed.to/buldwith/buildwith.csv', dtype='str')
# e = create_engine('mysql://root:rel8edto@localhost/buildwith?charset=latin1')

# dfall.to_sql('buildwithI', e)

# #
# pdf.columns
from sqlalchemy import create_engine
filenames = os.listdir('csvfiles')
filenames = [f for f in filenames if f.endswith('.csv')]
e = create_engine('mysql://root:rel8edto@localhost/buildwith')
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