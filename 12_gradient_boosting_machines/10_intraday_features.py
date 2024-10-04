
from pathlib import Path
import pandas as pd

algoseek_path = Path('/data/stocks/qqq/aggregates/algoseek')
nasdaq_raw_path = algoseek_path / 'nasdaq100'
files_itr = nasdaq_raw_path.glob('*/**/*.csv.gz')
files = list(files_itr)
files



f = files[0]
print(f)
df = pd.read_csv(f, nrows=1)  # Read only the first row for efficiency
df


keep = ['firsttradeprice', 
        'hightradeprice', 
        'lowtradeprice', 
        'lasttradeprice', 
        'minspread', 
        'maxspread',
        'volumeweightprice', 
        'nbboquotecount', 
        'tradeatbid', 
        'tradeatbidmid', 
        'tradeatmid', 
        'tradeatmidask', 
        'tradeatask', 
        'volume', 
        'totaltrades', 
        'finravolume', 
        'finravolumeweightprice', 
        'uptickvolume', 
        'downtickvolume', 
        'repeatuptickvolume', 
        'repeatdowntickvolume', 
        'tradetomidvolweight', 
        'tradetomidvolweightrelative']

drop_cols = ['unknowntickvolume',
             'cancelsize',
             'tradeatcrossorlocked']

tcols = ['openbartime', 
         'firsttradetime',
         'highbidtime', 
         'highasktime', 
         'hightradetime',
         'lowbidtime', 
         'lowasktime', 
         'lowtradetime',
         'closebartime', 
         'lasttradetime']

columns = {'volumeweightprice'          : 'price',
           'finravolume'                : 'fvolume',
           'finravolumeweightprice'     : 'fprice',
           'uptickvolume'               : 'up',
           'downtickvolume'             : 'down',
           'repeatuptickvolume'         : 'rup',
           'repeatdowntickvolume'       : 'rdown',
           'firsttradeprice'            : 'first',
           'hightradeprice'             : 'high',
           'lowtradeprice'              : 'low',
           'lasttradeprice'             : 'last',
           'nbboquotecount'             : 'nbbo',
           'totaltrades'                : 'ntrades',
           'openbidprice'               : 'obprice',
           'openbidsize'                : 'obsize',
           'openaskprice'               : 'oaprice',
           'openasksize'                : 'oasize',
           'highbidprice'               : 'hbprice',
           'highbidsize'                : 'hbsize',
           'highaskprice'               : 'haprice',
           'highasksize'                : 'hasize',
           'lowbidprice'                : 'lbprice',
           'lowbidsize'                 : 'lbsize',
           'lowaskprice'                : 'laprice',
           'lowasksize'                 : 'lasize',
           'closebidprice'              : 'cbprice',
           'closebidsize'               : 'cbsize',
           'closeaskprice'              : 'caprice',
           'closeasksize'               : 'casize',
           'firsttradesize'             : 'firstsize',
           'hightradesize'              : 'highsize',
           'lowtradesize'               : 'lowsize',
           'lasttradesize'              : 'lastsize',
           'tradetomidvolweight'        : 'volweight',
           'tradetomidvolweightrelative': 'volweightrel'}

df['date_timebarstart'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['TimeBarStart'].astype(str), format='%Y%m%d %H:%M')


df = (df.rename(columns=str.lower)
    .drop(tcols + drop_cols, axis=1)
    .rename(columns=columns)
    .set_index('date_timebarstart')
    .sort_index()
    .between_time('9:30', '16:00')
    .set_index('ticker', append=True)
    .swaplevel()
    .rename(columns=lambda x: x.replace('tradeat', 'at'))
)











# VALIDATE FILE COLUMN TYPES
print(nasdaq_raw_path)
files = list(nasdaq_raw_path.glob('2015/**/*.csv.gz'))
printed_types = set()
print("File count " + str(len(files)))

column_name = 'Date'  # Define column name variable
column_name = 'TimeBarStart'  # Define column name variable

for i, f in enumerate(files):
    df = pd.read_csv(f, nrows=1)  # Read only the first row for efficiency
    date_type = df[column_name].dtype
    if date_type not in printed_types:
        first_value = df[column_name].iloc[0]
        print(f"File {i+1}: {f}")
        print(f"Data type of '{column_name}' column: {date_type}")
        print(f"First value of '{column_name}' column: {first_value}\n")
        printed_types.add(date_type)





