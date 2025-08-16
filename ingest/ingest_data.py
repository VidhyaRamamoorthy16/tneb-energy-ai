import pandas as pd
from sqlalchemy.orm import Session
import logging
import os

# Import components from your existing 'app' package
from app.database import SessionLocal, engine
from app.models import Device, EnergyUsage # We need EnergyUsage for the duplicate check
from app import crud, schemas, models

# This line ensures that the database tables are created if they don't exist yet.
# It's good practice to have it here for a standalone script.
models.Base.metadata.create_all(bind=engine)

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the path to your data file
DATA_FILE_PATH = os.path.join("data", "raw_energy_data.csv")

def load_energy_data(db_session: Session):
    """
    Reads energy data from a CSV and loads it into the database,
    using the existing CRUD functions from the application.
    """
    try:
        df = pd.read_csv(DATA_FILE_PATH)
        logging.info(f"Successfully loaded {len(df)} rows from {DATA_FILE_PATH}")
    except FileNotFoundError:
        logging.error(f"Error: The file at {DATA_FILE_PATH} was not found.")
        return

    # --- Data Cleaning and Preparation ---
    df.columns = df.columns.str.strip()
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # --- Data Ingestion ---
    records_added = 0
    devices_created = 0

    for index, row in df.iterrows():
        # 1. Get or create the device using your CRUD functions
        device = crud.get_device_by_device_id(db=db_session, device_id=row['device_id'])
        if not device:
            logging.info(f"Device '{row['device_id']}' not found. Creating...")
            device_schema = schemas.DeviceCreate(device_id=row['device_id'], name=f"Device {row['device_id']}")
            device = crud.create_device(db=db_session, device=device_schema)
            devices_created += 1

        # 2. Check if this specific record already exists to avoid duplicates
        record_exists = db_session.query(EnergyUsage).filter(
            EnergyUsage.device_id == device.id,
            EnergyUsage.timestamp == row['timestamp']
        ).first()

        if record_exists:
            continue # Skip if this exact timestamp for this device is already in the DB

        # 3. Create the energy usage record using your CRUD function
        energy_usage_schema = schemas.EnergyUsageCreate(
            timestamp=row['timestamp'],
            kwh=row['energy_kwh']
        )
        crud.create_device_energy_usage(db=db_session, item=energy_usage_schema, device_id=device.id)
        records_added += 1

    logging.info(f"Ingestion complete.")
    logging.info(f"Created {devices_created} new device(s).")
    logging.info(f"Added {records_added} new energy usage records.")


if __name__ == "__main__":
    logging.info("Starting data ingestion process...")
    db = SessionLocal()
    try:
        load_energy_data(db)
    finally:
        db.close()
        logging.info("Database session closed.")
