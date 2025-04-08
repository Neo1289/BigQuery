import logging 
import os

logging.basicConfig(
    filename='logfile.txt',  
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


from scripts import ( 
    bitcoin_transactions,
    bitcoin_price
    )


def main() -> None:
    
    jobs = [
        bitcoin_transactions,
        bitcoin_price
    ]
    
    for job in jobs:
        job.run_etl()
        logger.info(job.run_etl.__doc__)
        
if __name__ == "__main__":
    main()