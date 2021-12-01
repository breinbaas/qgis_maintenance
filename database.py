from pydantic import BaseModel
import os

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, UnicodeText, Numeric, create_engine, func, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import except_
from sqlalchemy.sql.schema import MetaData
from pyproj import Transformer
from typing import List, Union
from pathlib import Path

transformer = Transformer.from_crs(28992, 4326, always_xy=True)
Base = declarative_base()

class DB_CPT(Base):
    __tablename__ = "cpts"
    id = Column(Integer, primary_key=True)
    owner = Column(String, nullable=False)
    name = Column(String, nullable=False)
    x = Column(Numeric(precision=8, scale=2, asdecimal=True), nullable=False)
    y = Column(Numeric(precision=8, scale=2, asdecimal=True), nullable=False)
    z = Column(Numeric(precision=8, scale=2, asdecimal=True), nullable=False)
    lat = Column(Numeric(precision=12, scale=10, asdecimal=True), nullable=False) 
    lon = Column(Numeric(precision=12, scale=10, asdecimal=True), nullable=False)
    date = Column(String, nullable=False)
    raw = Column(UnicodeText, nullable=False)

    

class CPTMetaData(BaseModel):
    name: str = ""
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    lat: float = 0.0
    lon: float = 0.0
    date: str = ""

    @classmethod
    def from_gef(cls, geffile):  
        result = CPTMetaData()
        srid = 0
        lines = open(geffile, 'r').readlines()
        try:
            for line in lines:
                if line.find('#XYID') > -1:
                    args = line.split('=')[-1].split(',')
                    srid = int(args[0])
                    result.x = float(args[1])
                    result.y = float(args[2])
                    if srid == 31000:
                        result.lon, result.lat = transformer.transform(result.x, result.y)
                    else:
                        raise ValueError(f"Unknown SRID '{srid}' in Cpt file '{geffile}'")
                elif line.find("#ZID") > -1:
                    args = line.split('=')[-1].split(',')
                    srid = int(args[0])
                    if srid == 31000:
                        result.z = float(args[1])  
                    else:
                        raise ValueError(f"Unknown SRID '{srid}' in Cpt file '{geffile}'")
                elif line.find("#TESTID")>-1:
                    args = line.split('=')[-1]
                    result.name = args.strip()
                elif line.find("#STARTDATE")>-1:
                    yyyy, mm, dd = [int(e) for e in line.split('=')[-1].split(',')]
                    result.date = f"{yyyy:04d}{mm:02d}{dd:02d}"
        except Exception as e:
            raise ValueError(f"Cannot create CPTMetaData from file '{geffile}', got error '{e}'")

        return result
                


                
        

class Database:
    path_log: Union[Path, str]
    engine = None
    session = None
    is_connected = False

    def connect(self, username, password, hostname, port, databasename):
        try:
            self.engine = create_engine(f'postgresql://{username}:{password}@{hostname}:{port}/{databasename}', echo=False)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            self.engine.connect()
            self.is_connected = True
        except Exception as e:
            self.is_connected = False
            raise ValueError(f"Could not login to database, got error '{e}'")

    def disconnect(self):
        if self.session:
            self.session.close()
        self.engine.dispose()
        self.engine = None
        self.session = None
        self.is_connected = False

    def create(self):    
        insp = inspect(self.engine)
        if not insp.has_table("cpts"):    
            DB_CPT.__table__.create(self.engine)

    # ADD
    def add_cpt(self, owner: str, cpt_metadata: CPTMetaData, cptfile):        
        try:
            newcpt = DB_CPT(
                owner = owner,
                name = cpt_metadata.name,    
                x = cpt_metadata.x,
                y = cpt_metadata.y,
                z = cpt_metadata.z,
                lat = cpt_metadata.lat,
                lon = cpt_metadata.lon,
                date = cpt_metadata.date,
                raw = open(cptfile, 'r').read()    
            )  
            self.session.add(newcpt)  
            self.session.commit()      
        except Exception as e:
            print(e)
            #TODO log
            #logging.error(f"Error adding cpt from file '{cptfile}', got error '{e}'")
            self.session.rollback()

    # GET
    def get_cpt_metadata(self, owner: str) -> List[CPTMetaData]:
        """Get all cpt metadata from the database
        
        Args:
            owner (str): name of the owner

        Returns:
            List[CPTMetaData]: list with the cpt's that match the given parameters
        """
        
        query = self.session.query(DB_CPT).\
            filter(DB_CPT.owner == owner)
        
        result = []
        for instance in query:
            try:
                cptm = CPTMetaData(
                    name = instance.name,
                    x = instance.x,
                    y = instance.y,
                    z = instance.z,
                    lat = instance.lat,
                    lon = instance.lon,
                    date = instance.date,
                )
                result.append(cptm)
            except Exception as e:
                print(e)
                #TODO logging is nice
                #logging.warning(f"Error convert raw cpt data to CPT from '{instance.id}', got error '{e}'")

        return result



