from sqlalchemy import Column, String, Integer, Numeric, TIMESTAMP, ForeignKey, BigInteger, create_engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Feeder(Base):
    __tablename__ = 'assets_feeder'
    feeder_id = Column(String, primary_key=True)
    name = Column(String)
    city = Column(String)
# ... (add DT, Consumer, MeterRead, FeederEnergy, DTEnergy, StreetlightEnergy, Case classes here as described earlier)
