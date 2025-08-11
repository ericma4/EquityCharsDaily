import pandas as pd
import numpy as np
import datetime as dt
import wrds
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
import datetime
import pyarrow.feather as feather

###################
# Connect to WRDS #
###################
conn = wrds.Connection()

# CRSP Block
crsp = conn.raw_sql("""
                      select a.permno, a.dlycaldt, a.dlyret, a.dlyretx, (a.dlyret - b.rf) as exret, b.mktrf, b.smb, b.hml, a.dlyvol, a.dlyprc, 
                             a.shrout, a.dlylow, a.dlyhigh
                      from crsp.dsf_v2 as a
                      left join ff.factors_daily as b
                      on a.dlycaldt=b.date
                      where a.dlycaldt >= '01/01/1959'
                      """, date_cols=['dlycaldt'])

# crsp = crsp.dropna()

with open('crsp_dsf_1959.feather', 'wb') as f:
    feather.write_feather(crsp, f)
