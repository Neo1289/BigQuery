import logging 
import os
from google.oauth2 import service_account

logging.basicConfig(
    filename='logfile.txt',  
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


from scripts import ( 
    bitcoin_transactions,
    bitcoin_price,
    fred
    )


def main() -> None:
    credentials = service_account.Credentials.from_service_account_file("connection-123-892e002c2def.json")

    jobs = [
        bitcoin_transactions,
        bitcoin_price,
        fred
    ]
    
    for job in jobs:
        job.run_etl(credentials)
        logger.info(job.run_etl.__doc__)
        
if __name__ == "__main__":
    main()