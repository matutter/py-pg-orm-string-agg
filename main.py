from sqlalchemy import create_engine, func, select, Column, Integer, Sequence, LargeBinary, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import aggregate_order_by, BYTEA

dsn = "postgres://postgres:postgres@localhost/test"
Base = declarative_base()
engine = create_engine(dsn, echo=0)

class Blob(Base):
  __tablename__ = 'blobs'
  _id = Column(Integer, Sequence('blobs_id_seq'), primary_key=True, unique=True)
  ch = Column(Integer)
  seq = Column(Integer)
  data = Column(LargeBinary)
  def __repr__(self):
    return f"<{type(self).__name__}({self._id},d:{self.data},ch:{self.ch},seq:{self.seq})>"

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

db = Session(engine)
db.add(Blob(data=b'....aa....', ch=0, seq=0))
db.add(Blob(data=b'....bb....', ch=1, seq=1))
db.add(Blob(data=b'....cc....', ch=1, seq=0))
db.commit()

list(map(print,db.query(Blob)))
channel = 1
filter_ = and_(Blob.ch == channel)
db.add(Blob(data=db.query(func.string_agg(Blob.data,
  aggregate_order_by(None, Blob.seq.asc()))).filter( filter_ )))
db.query(Blob).filter(filter_).delete(synchronize_session='fetch')
db.commit()

print('============= AFTER')

list(map(print,db.query(Blob)))
