#!/usr/bin/env python

from sqlalchemy import *
from sqlalchemy.orm import *

engine = create_engine('postgresql://blitzortung:blitzortung@localhost/blitzortung', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from geoalchemy import *

metadata = MetaData(engine)
Base = declarative_base(metadata=metadata)

class Strokes(Base):
    __tablename__ = 'strokes'
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    geom = GeometryColumn(Point(2))
    height = Column(Integer)
    stationcount = Column(Integer)
    error = Column(Float)

class Road(Base):
    __tablename__ = 'roads'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False)
    width = Column(Integer)
    created = Column(DateTime, default=datetime.now())
    geom = GeometryColumn(LineString(2))

class Lake(Base):
    __tablename__ = 'lakes'
    id = Column(Integer, primary_key=True)
    name = Column(Unicode, nullable=False)
    depth = Column(Integer)
    created = Column(DateTime, default=datetime.now())
    geom = GeometryColumn(Polygon(2))

GeometryDDL(Strokes.__table__)
GeometryDDL(Road.__table__)
GeometryDDL(Lake.__table__)

metadata.drop_all()
metadata.create_all()
