from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

user='kali'
passwd='kali123'

stock = create_engine(f"mysql+pymysql://{user}:{passwd}@127.0.0.1/stock?charset=utf8", poolclass=NullPool,
                connect_args={"charset": "utf8", "connect_timeout": 10})
fund = create_engine(f"mysql+pymysql://{user}:{passwd}@127.0.0.1/fund?charset=utf8", poolclass=NullPool,
                connect_args={"charset": "utf8", "connect_timeout": 10})
factor = create_engine(f"mysql+pymysql://{user}:{passwd}@127.0.0.1/factor?charset=utf8", poolclass=NullPool,
                connect_args={"charset": "utf8", "connect_timeout": 10})