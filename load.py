from sqlalchemy import create_engine
import pandas as pd

data = pd.read_csv('No missing data values.csv')
engine = create_engine('postgresql://postgres:root123@localhost:5432/TestDatabase')
data.to_sql('CompanySeries', engine)
