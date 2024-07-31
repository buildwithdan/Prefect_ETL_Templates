import logging
import requests
from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine import Engine
from prefect import task, flow
from prefect.blocks.system import Secret

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the API endpoint and retrieve API key from secrets
api_url = 'http://api.weatherapi.com/v1/current.json'
api_key_raw = Secret.load("weather-api-key").get()

# Define the connection parameters for the database
db_host = Secret.load("db-host").get()
db_user = Secret.load("db-user").get()
db_password = Secret.load("db-password").get()
db_name = "PrivateDB"

# Create SQLAlchemy base
Base = declarative_base()

# Define the Weather model
class Weather(Base):
    __tablename__ = 'weather_data'
    __table_args__ = {'schema': 'weather'}
    last_updated_epoch = Column(Integer, primary_key=True)
    last_updated = Column(String)
    temp_c = Column(Float)
    temp_f = Column(Float)
    is_day = Column(Boolean)
    condition_text = Column(String)
    condition_icon = Column(String)
    condition_code = Column(Integer)
    wind_mph = Column(Float)
    wind_kph = Column(Float)
    wind_degree = Column(Integer)
    wind_dir = Column(String)
    pressure_mb = Column(Float)
    pressure_in = Column(Float)
    precip_mm = Column(Float)
    precip_in = Column(Float)
    humidity = Column(Integer)
    cloud = Column(Integer)
    feelslike_c = Column(Float)
    feelslike_f = Column(Float)
    windchill_c = Column(Float)
    windchill_f = Column(Float)
    heatindex_c = Column(Float)
    heatindex_f = Column(Float)
    dewpoint_c = Column(Float)
    dewpoint_f = Column(Float)
    vis_km = Column(Float)
    vis_miles = Column(Float)
    uv = Column(Float)
    gust_mph = Column(Float)
    gust_kph = Column(Float)


def get_engine_db(bulk: bool = True) -> Engine:
    # Create the connection string for SQLAlchemy
    con_str = (
        f"mssql+pyodbc://{db_user}:{db_password}@{db_host}/{db_name}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&Encrypt=yes"
        "&TrustServerCertificate=no"
        "&Connection Timeout=30"
    )
    return create_engine(con_str, fast_executemany=bulk, echo=True)

@task
def setup_db():
    engine = get_engine_db()
    Base.metadata.create_all(engine)
    logger.info("Database setup completed successfully.")


@task
def get_data(url, api_key):
    try:
        response = requests.get(url, 
            params={'key': api_key, 'q': 'Maidstone'}
            )

        response.raise_for_status()  # Raise an HTTPError for bad responses

        data = response.json().get("current")

        logger.info("Data collected successfully")

        return data

    except requests.RequestException as e:
        logger.error(f"Error fetching data from API: {e}", exc_info=True)
        return data

@flow
def insert_data():
    data = get_data(url=api_url, api_key=api_key_raw)
    if not data:
        logger.warning("No data to insert")
        return

    engine = get_engine_db()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Create a Weather instance with the data
    weather = Weather(
        last_updated_epoch=data['last_updated_epoch'],
        last_updated=data['last_updated'],
        temp_c=data['temp_c'],
        temp_f=data['temp_f'],
        is_day=data['is_day'],
        condition_text=data['condition']['text'],
        condition_icon=data['condition']['icon'],
        condition_code=data['condition']['code'],
        wind_mph=data['wind_mph'],
        wind_kph=data['wind_kph'],
        wind_degree=data['wind_degree'],
        wind_dir=data['wind_dir'],
        pressure_mb=data['pressure_mb'],
        pressure_in=data['pressure_in'],
        precip_mm=data['precip_mm'],
        precip_in=data['precip_in'],
        humidity=data['humidity'],
        cloud=data['cloud'],
        feelslike_c=data['feelslike_c'],
        feelslike_f=data['feelslike_f'],
        windchill_c=data['windchill_c'],
        windchill_f=data['windchill_f'],
        heatindex_c=data['heatindex_c'],
        heatindex_f=data['heatindex_f'],
        dewpoint_c=data['dewpoint_c'],
        dewpoint_f=data['dewpoint_f'],
        vis_km=data['vis_km'],
        vis_miles=data['vis_miles'],
        uv=data['uv'],
        gust_mph=data['gust_mph'],
        gust_kph=data['gust_kph']
    )

    # Add the Weather instance to the session and commit
    try:
        session.add(weather)
        session.commit()
        logger.info("Data submitted to database correctly")
    except Exception as e:
        logger.error("Failed to insert data into the database", exc_info=True)
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    # Uncomment the line below if you want to setup the database schema
    setup_db()
    insert_data()