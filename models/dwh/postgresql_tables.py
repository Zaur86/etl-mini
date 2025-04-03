from sqlalchemy import Column, String, JSON, Integer, TIMESTAMP, Date, func
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class FromELasticTempTable(Base):

    __tablename__ = "from_elastic_temp"

    id = Column(String(64), primary_key=True)
    timestamp = Column(String(64), nullable=False)
    data = Column(JSON, nullable=False)
    datetime = Column(TIMESTAMP(0), nullable=False)
    time_mcs = Column(Integer, nullable=False)
    source = Column(JSON, nullable=False)
    loaded_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    month_ = Column(Date, nullable=True)
    version_ = Column(String(256), nullable=True)

    @classmethod
    def create_with_name(cls, table_name):
        return type(table_name, (Base,), {
            "__tablename__": table_name,
            "id": Column(String(64), primary_key=True),
            "timestamp": Column(String(64), nullable=False),
            "data": Column(JSON, nullable=False),
            "datetime": Column(TIMESTAMP(0), nullable=False),
            "time_mcs": Column(Integer, nullable=False),
            "source": Column(JSON, nullable=False),
            "loaded_at": Column(TIMESTAMP, server_default=func.current_timestamp()),
            "month_": Column(Date, nullable=True),
            "version_": Column(String(256), nullable=True),
        })
