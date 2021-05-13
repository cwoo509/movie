  
from sqlalchemy import create_engine

def conn():
    conn = create_engine('oracle+cx_oracle://farmai:123456@193.122.124.189:1521/xe')

    return conn