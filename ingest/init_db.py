from sqlalchemy import create_engine
from models import Base

engine = create_engine("postgresql+psycopg2://tnebuser:secret123@localhost:5432/tnebenergyai")
Base.metadata.create_all(engine)
