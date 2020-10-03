from sqlalchemy import create_engine, func, select, Column, Integer, Sequence, LargeBinary
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import aggregate_order_by, BYTEA

dsn = "postgres://postgres:postgres@localhost/test"
Base = declarative_base()
engine = create_engine(dsn, echo=True)

class Blob(Base):
  __tablename__ = 'blobs'
  _id = Column(Integer, Sequence('blobs_id_seq'), primary_key=True, unique=True)
  data = Column(LargeBinary)

  def __repr__(self):
    return f"<{type(self).__name__}({self._id},d:{self.data})>"

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

db = Session(engine)
db.add(Blob(data=b'....aa....'))
db.add(Blob(data=b'....bb....'))
db.add(Blob(data=b'....cc....'))
db.commit()

for blob in db.query(Blob):
  print(blob)

filter_ = Blob._id.in_([1, 2])
db.add(Blob(data=db.query(func.string_agg(Blob.data,
  aggregate_order_by(None, Blob._id.desc()))).filter( filter_ )))
db.query(Blob).filter(filter_).delete(synchronize_session='fetch')
db.commit()

for blob in db.query(Blob):
  print(blob)
