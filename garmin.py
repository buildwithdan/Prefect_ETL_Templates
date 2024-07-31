from garminconnect import (
  Garmin,
  GarminConnectAuthenticationError,
  GarminConnectConnectionError,
  GarminConnectTooManyRequestsError
)
import pandas as pd
import time 

from datetime import datetime, timedelta
import requests
from getpass import getpass
from garth.exc import GarthHTTPError

from garmin_models import *

from prefect import task, flow
from prefect.blocks.system import Secret

garmin_email = Secret.load("garmin-email").get()
garmin_password = Secret.load("garmin-pwd").get()
api = None
tokenstore = ".garminconnect"

# @task(log_prints=True)
def init_api(email, password):
  try:
    print(
      f"Trying to login to Garmin Connect using token data from '{tokenstore}'...\n"
    )
    garmin = Garmin()
    garmin.login(tokenstore)
  except (FileNotFoundError, GarthHTTPError, GarminConnectAuthenticationError):
    # print(
    #   "Login tokens not present, login with your Garmin Connect credentials to generate them.\n"
    #   f"They will be stored in '{tokenstore}' for future use.\n"
    # )
    try:
      garmin = Garmin(garmin_email, garmin_password)
      garmin.login()
      # Save tokens for next login
      garmin.garth.dump(tokenstore)
    except (FileNotFoundError, GarthHTTPError, GarminConnectAuthenticationError, requests.exceptions.HTTPError) as err:
      logger.error(err)
      return None
  return garmin


if not api:
  api = init_api(garmin_email, garmin_password)


# Inserting into DB via SQLalchemy

def insert_steps(data):  # Keep the function name as is
    session = Session()
    try:
        for entry in data:
            row = Steps_tbl(  # Assuming StepsData is your model
                startGMT=datetime.strptime(entry["startGMT"], "%Y-%m-%dT%H:%M:%S.%f"),
                endGMT=datetime.strptime(entry["endGMT"], "%Y-%m-%dT%H:%M:%S.%f"),
                steps=entry["steps"],
                pushes=entry["pushes"],
                primaryActivityLevel=entry["primaryActivityLevel"],
                activityLevelConstant=entry["activityLevelConstant"]
            )
            session.add(row)
        
        session.commit()
        print("Step data successfully inserted into the database.")
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()
        print("Session closed")
        
# Inserting into DB via Pandas - working  
          
def insert_df_steps(data):
    try:
        # Convert the list of dictionaries into a DataFrame
        df = pd.DataFrame(data)
        
        df['activityLevelConstant'] = df['activityLevelConstant'].astype(int)
        df['startGMT'] = pd.to_datetime(df['startGMT'])
        df['endGMT'] = pd.to_datetime(df['endGMT'])
        # print(df)


        df.to_sql('etl_steps', con=engine, schema=f"{db_schema}" ,if_exists='append', index=False)
        # print("Step data successfully inserted into the database.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Operation completed.")


def insert_df_hrate_per_min(data):
    try:
        # Convert the list of dictionaries into a DataFrame
        df = pd.DataFrame(data)
        df.rename(columns={0: 'timestamp', 1: 'heartrate'}, inplace=True)

        # Convert timestamp from Unix time in milliseconds to datetime
        # Adjust the unit to 's' if your timestamp is in seconds
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')

        # print(df)


        # # Insert the DataFrame into SQL database, assuming 'step_tbl' is your table name
        
        
        df.to_sql('etl_hrate_min', con=engine, schema=f"{db_schema}" ,if_exists='append', index=False)
        # # print("Step data successfully inserted into the database.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Operation completed.")    


def insert_df_hrate_per_day(data):
    try: 
        data.pop('heartRateValueDescriptors', None)
        data.pop('heartRateValues', None)
        
        df = pd.DataFrame([data])
        
        # print(df)
        
        # Insert the DataFrame into the SQL database
        df.to_sql('etl_hrate_day', con=engine, schema=db_schema, if_exists='append', index=False)
        
        print("Data successfully inserted into the database.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Operation completed.") 


def insert_df_stress_per_min(data):
    try:
        # Convert the list of dictionaries into a DataFrame
        df = pd.DataFrame(data)
        df.rename(columns={0: 'timestamp', 1: 'stressLevel'}, inplace=True)

        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True).dt.strftime('%Y-%m-%d %H:%M:%S')

        # print(df)
        
        df.to_sql('etl_stress_min', con=engine, schema=f"{db_schema}" ,if_exists='append', index=False)
        # # print("Step data successfully inserted into the database.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Operation completed.")    


def insert_df_stress_per_day(data):
    try: 
        data.pop('stressChartValueOffset', None)
        data.pop('stressChartYAxisOrigin', None)
        data.pop('stressValueDescriptorsDTOList', None)
        data.pop('stressValuesArray', None)
        data.pop('bodyBatteryValueDescriptorsDTOList', None)
        data.pop('bodyBatteryValuesArray', None)
        
        df = pd.DataFrame([data])
        
        # print(df)
        
        # Insert the DataFrame into the SQL database
        df.to_sql('etl_stress_day', con=engine, schema=db_schema, if_exists='append', index=False)
        
        print("Data successfully inserted into the database.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Operation completed.") 


@flow(name="Garmin Data Flow")
def logic_flow():
  # Day Loops
  start_date = (datetime.today() - timedelta(days=30)).date()
  end_date = datetime.today().date()
  current_date = start_date
  
  # Timer starts now
  start_time = time.time()
  

  # Testing API endpoints:::
  # info = api.get_full_name()
  # info2 = api.get_blood_pressure(start_date)
  # info2 = api.get_daily_weigh_ins(start_date)
  # info2 = api.get_weigh_ins(start_date, end_date)
  # info3 = api.get_user_profile()
  # info3 = api.get_stress_data(start_date)
  # info3 = api.get_heart_rates(start_date)
  # info3 = api.get_sleep_data(start_date)

  # Loop starts
  while current_date <= end_date:
    
    steps_data = api.get_steps_data(current_date)
    insert_df_steps(steps_data)
    
    hrate_data = api.get_heart_rates(current_date)
    insert_df_hrate_per_min(hrate_data["heartRateValues"])
    insert_df_hrate_per_day(hrate_data)

    stress_data = api.get_stress_data(current_date)
    insert_df_stress_per_min(stress_data["stressValuesArray"])
    insert_df_stress_per_day(stress_data)
   
    
    # body_battery = api.get_body_battery(current_date)
    
    # floors = api.get_floors(current_date)
    
    # rhr_day = api.get_rhr_day(current_date)
    
    # sleep_data = api.get_sleep_data(current_date)
    
    # sdfsdf
    
    # respiration_data = api.get_respiration_data(current_date)
    
    # spo2_data = api.get_spo2_data(current_date)
    # max_metrics = api.get_max_metrics(current_date)
    
    current_date += timedelta(days=1)
    print(current_date)
    print(f"Completed: {current_date}") 
    
  end_time = time.time()
  time_taken = end_time - start_time
  print(f"Completed Sync: {time_taken} seconds")



if __name__ == "__main__":
    logic_flow()