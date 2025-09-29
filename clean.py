import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

def clean():
    con = None
    try:
        con = duckdb.connect(database='traffic.duckdb', read_only=False)

        ## Remove duplicate rows
        con.execute(f"""
                
                """)
    