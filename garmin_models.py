from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Engine, engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import URL
import pyodbc
from prefect.blocks.system import Secret

db_schema = "garmin"

# Load secrets
db_host = Secret.load("db-host").get()
db_user = Secret.load("db-user").get()
db_password = Secret.load("db-password").get()
db_name = "PrivateDB"
db_port = "1433"

Base = declarative_base()

# class BodyBattery_tbl(Base):
#     __tablename__ = 'etl_body_battery'
#     __table_args__ = {'schema': f"{db_schema}"}
#     date = Column(Date)
#     charged = Column(Integer)
#     drained = Column(Integer)
#     start_timestamp_gmt = Column(DateTime, primary_key=True)
#     end_timestamp_gmt = Column(DateTime)
#     body_battery_values = Column(JSON)  # Storing the array directly; consider normalization

# class Floor_tbl(Base):
#     __tablename__ = 'etl_floor'
#     __table_args__ = {'schema': f"{db_schema}"}
#     start_gmt = Column(DateTime, primary_key=True)
#     end_gmt = Column(DateTime)
#     floors_ascended = Column(Integer)
#     floors_descended = Column(Integer)

class Hrate_day_tbl(Base):
    __tablename__ = 'etl_hrate_day'
    __table_args__ = {'schema': f"{db_schema}"}
    userProfilePK = Column(Integer)
    calendarDate = Column(DateTime, primary_key=True)
    startTimestampGMT = Column(DateTime)
    endTimestampGMT = Column(DateTime)
    startTimestampLocal = Column(DateTime)
    endTimestampLocal = Column(DateTime)
    maxHeartRate = Column(Integer)
    minHeartRate = Column(Integer)
    restingHeartRate = Column(Integer)
    lastSevenDaysAvgRestingHeartRate = Column(Integer)


class Hrate_min_tbl(Base):
    __tablename__ = 'etl_hrate_min'
    __table_args__ = {'schema': f"{db_schema}"}
    timestamp = Column(DateTime, primary_key=True)
    heartrate = Column(Integer)
    

class Stress_day_tbl(Base):
    __tablename__ = 'etl_stress_day'
    __table_args__ = {'schema': f"{db_schema}"}
    userProfilePK = Column(Integer)
    calendarDate = Column(DateTime, primary_key=True)
    startTimestampGMT = Column(DateTime)
    endTimestampGMT = Column(DateTime)
    startTimestampLocal = Column(DateTime)
    endTimestampLocal = Column(DateTime)
    maxStressLevel = Column(Integer)
    avgStressLevel = Column(Integer)


class Stress_min_tbl(Base):
    __tablename__ = 'etl_stress_min'
    __table_args__ = {'schema': f"{db_schema}"}
    timestamp = Column(DateTime, primary_key=True)
    stressLevel = Column(Integer)


# class MaxMetrics_tbl(Base):
#     __tablename__ = 'etl_maxmetrics'
#     __table_args__ = {'schema': f"{db_schema}"}
#     calendar_date = Column(DateTime, primary_key=True)
#     vo2_max_precise_value = Column(Float)
#     fitness_age = Column(Integer)
#     heat_acclimation_percentage = Column(Float)
#     # Add more fields as needed from maxmetrics.json
 
# class Respiration_tbl(Base):
#     __tablename__ = 'etl_respiration'
#     __table_args__ = {'schema': f"{db_schema}"}
#     start_timestamp_gmt = Column(DateTime, primary_key=True)
#     end_timestamp_gmt = Column(DateTime)
#     # Include specific fields for respiration data 

# class RestingHeart_tbl(Base):
#     __tablename__ = 'etl_resting_heart'
#     __table_args__ = {'schema': f"{db_schema}"}
#     user_profile_id = Column(Integer)
#     statistics_start_date = Column(Date, primary_key=True)
#     statistics_end_date = Column(Date)
#     value = Column(Float)
#     calendar_date = Column(Date)

# class Sleep_tbl(Base):
#     __tablename__ = 'etl_sleep'
#     __table_args__ = {'schema': f"{db_schema}"}
#     start_gmt = Column(DateTime, primary_key=True)
#     end_gmt = Column(DateTime)
#     sleep_level = Column(String)
#     # Add more fields as per your sleep.json structure

# class SpO2_tbl(Base):
#     __tablename__ = 'etl_spo2'
#     __table_args__ = {'schema': f"{db_schema}"}
#     start_timestamp_gmt = Column(DateTime, primary_key=True)
#     end_timestamp_gmt = Column(DateTime)
#     average_spo2 = Column(Float)
#     lowest_spo2 = Column(Integer)
#     spo2_hourly_averages = Column(JSON)  # Storing JSON directly, consider normalization

class Steps_tbl(Base):
    __tablename__ = 'etl_steps'
    __table_args__ = {'schema': f"{db_schema}"}
    startGMT = Column(DateTime, primary_key=True)
    endGMT = Column(DateTime)
    steps = Column(Integer)
    pushes = Column(Integer)
    primaryActivityLevel = Column(String)
    activityLevelConstant = Column(Integer)


# class personal_records_tbl(Base):
#     __tablename__ = 'etl_personal_records'
#     id = Column(Integer, primary_key=True)
#     typeId = Column(Integer)
#     activityId = Column(Integer)
#     activityName = Column(String)
#     activityType = Column(String)
#     activityStartDateTimeInGMT = Column(DateTime)
#     actStartDateTimeInGMTFormatted = Column(String)
#     activityStartDateTimeLocal = Column(DateTime)
#     activityStartDateTimeLocalFormatted = Column(String)
#     value = Column(Float)
#     prStartTimeGmt = Column(Integer)
#     prStartTimeGmtFormatted = Column(String)
#     prStartTimeLocal = Column(Integer)
#     prStartTimeLocalFormatted = Column(String)
#     prTypeLabelKey = Column(String)
#     poolLengthUnit = Column(String)


def get_engine_db(bulk: bool=True) -> Engine:
    con_str = engine.URL.create(
        "mssql+pyodbc",
        username=db_user,
        password=db_password,
        host=db_host,
        database=db_name,
        port=db_port,
        query={
            "driver": 'ODBC Driver 17 for SQL Server',
            "LongAsMax": "Yes",
        }
    )
    return create_engine(con_str, fast_executemany=bulk, echo=True)

engine = get_engine_db()

Session = sessionmaker(bind=engine)

def setup_db():
    try:
        Base.metadata.create_all(engine)
        logger.info("Database setup completed successfully.")
    except Exception as e:
        logger.error("Database setup failed:", exc_info=True)



if __name__ == "__main__":
    logger.info("Starting database setup.")
    setup_db()