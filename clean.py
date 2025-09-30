import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

DB_FILE = "traffic.duckdb"

def clean():
    con = None
    try:
        con = duckdb.connect(database=DB_FILE, read_only=False)
        logger.info(f"Connected to database: {DB_FILE}")

        
        yellow_original_count = con.execute(f"""
                    SELECT COUNT(*) FROM yellow
                """)

        green_original_count = con.execute(f"""
                    SELECT COUNT(*) FROM green
                """)

        ## Remove duplicate rows
        logger.info("Removing duplicate rows...")
        con.execute(f"""
                    CREATE TABLE yellow_clean AS
                    SELECT DISTINCT * FROM yellow;

                    DROP TABLE yellow;
                    ALTER TABLE yellow_clean RENAME TO yellow;
                """)

        con.execute(f"""
                    CREATE TABLE green_clean AS
                    SELECT DISTINCT * FROM green;

                    DROP TABLE green;
                    ALTER TABLE green_clean RENAME TO green;
                """)
        

        logger.info("Dropped duplicate rows")

        logger.info("Deleting null value rows...")
        con.execute(f"""
                    DELETE FROM yellow
                    WHERE 
                    
                    VendorID IS NULL AND 
                    tpep_pickup_datetime IS NULL AND 
                    tpep_dropoff_datetime IS NULL AND
                     store_and_fwd_flag IS NULL AND 
                     RatecodeID IS NULL AND 
                     PULocationID IS NULL AND 
                     DOLocationID IS NULL AND
                     passenger_count IS NULL 
                     AND trip_distance IS NULL AND 
                     fare_amount IS NULL AND 
                     extra IS NULL 
                     AND mta_tax IS NULL AND
                     tip_amount IS NULL AND 
                     tolls_amount IS NULL AND 
                     improvement_surcharge IS NULL AND
                     total_amount IS NULL AND 
                     payment_type IS NULL AND 
                     congestion_surcharge IS NULL AND 
                     airport_fee IS NULL
                                     
                    ;
                """)
        con.execute(f"""
                    DELETE FROM green
                    WHERE 
                    
                    VendorID IS NULL AND 
                    lpep_pickup_datetime IS NULL AND 
                    lpep_dropoff_datetime IS NULL AND
                     store_and_fwd_flag IS NULL AND 
                     RatecodeID IS NULL AND 
                     PULocationID IS NULL AND 
                     DOLocationID IS NULL AND
                     passenger_count IS NULL AND 
                     trip_distance IS NULL AND 
                     fare_amount IS NULL AND 
                     extra IS NULL AND 
                     mta_tax IS NULL AND
                     tip_amount IS NULL AND 
                     tolls_amount IS NULL AND 
                     ehail_fee IS NULL AND 
                     improvement_surcharge IS NULL AND
                     total_amount IS NULL AND 
                     payment_type IS NULL AND 
                     trip_type IS NULL AND 
                     congestion_surcharge IS NULL 
                     ;
                """)


        logger.info("Deleted")

        con.execute(f"""
                    DELETE FROM yellow
                    WHERE passenger_count = 0
                """)

        con.execute(f"""
                    DELETE FROM green
                    WHERE passenger_count = 0
                """)

        con.execute(f"""
                    DELETE FROM yellow
                    WHERE trip_distance = 0
                """)

        con.execute(f"""
                    DELETE FROM green
                    WHERE trip_distance = 0
                """)

        con.execute(f"""
                    DELETE FROM yellow
                    WHERE trip_distance > 100
                """)

        con.execute(f"""
                    DELETE FROM green
                    WHERE trip_distance > 100
                """)

        con.execute(f"""
                    DELETE FROM yellow
                    WHERE date_diff('hour', tpep_dropoff_datetime - tpep_pickup_datetime) > 24
                """)

        con.execute(f"""
                    DELETE FROM green
                    WHERE date_diff('hour', lpep_dropoff_datetime - lpep_pickup_datetime) > 24
                """)

        logger.info("Cleaning executed")
        logger.info("Testing to see if cleaning succeeded...")

        yellow_new_size = con.execute(f"""
                    SELECT COUNT(*) from yellow;
                """)
        green_new_size = con.execute(f"""
                    SELECT COUNT(*) from green;
                """)

        logger.info("# of duplicate yellow trips dropped: ", yellow_original_count - yellow_new_size)
        logger.info("# of duplicate green trips dropped: ", green_original_count - green_new_size)

        logger.info("# of 0 passengers in yellow: ", 
        con.execute(f"""
                    SELECT COUNT(*) FROM yellow
                    WHERE passenger_count = 0;
                """)
                )

        logger.info("# of 0 passengers in green: ",
        con.execute(f"""
                    SELECT COUNT(*) FROM green
                    WHERE passenger_count = 0;
                """)
                )

        logger.info("# of 100 passengers in yellow: ",
        con.execute(f"""
                    SELECT COUNT(*) FROM yellow
                    WHERE passenger_count = 100;
                """)
                )

        logger.info("# of 100 passengers in green: ",
        con.execute(f"""
                    SELECT COUNT(*) FROM green
                    WHERE passenger_count = 100;
                """)
                )

        logger.info("# of NULL passengers in green: ",
        con.execute(f"""
                    SELECT COUNT(*) FROM green
                    WHERE passenger_count IS NULL;
                """)
                )
                

        logger.info("# of NULL passengers in yellow: ",
        con.execute(f"""
                    SELECT COUNT(*) FROM yellow
                    WHERE passenger_count IS NULL;
                """)
                )

        logger.info("# of yellow trips > 24 hrs: ",
        con.execute(f"""
                    SELECT COUNT(*) FROM yellow
                    WHERE date_diff('hour', tpep_dropoff_datetime - tpep_pickup_datetime) > 24;
                """)
                )

        logger.info("# of green trips > 24 hrs: ",
        con.execute(f"""
                    SELECT COUNT(*) FROM green
                    WHERE date_diff('hour', tpep_dropoff_datetime - tpep_pickup_datetime) > 24;
                """)
                )

    except Exception as e:
        logger.critical(f"A critical error occurred in the main script: {e}")
    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")

if __name__ == "__main__":
    clean()