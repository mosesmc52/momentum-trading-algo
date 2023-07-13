from database import Base
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship


class Security(Base):
    __tablename__ = "security"

    id = Column(Integer, primary_key=True)
    type = Column(String(20))
    name = Column(String(20))
    ticker = Column(String(10), unique=True)
    prices = relationship("Price")

    def __init__(self, security_type=None, name=None, type=None, ticker=None):
        self.name = name
        self.ticker = ticker
        self.type = type


class Price(Base):
    __tablename__ = "price"
    id = Column(Integer, primary_key=True)
    security_id = Column(Integer, ForeignKey("security.id"))
    close = Column(Float)
    date = Column(DateTime)

    def __init__(self, security_id, close=None, date=None):
        self.close = close
        self.date = date
        self.security_id = security_id
