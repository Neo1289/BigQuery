import logging 
import os

from scripts import ( 
    bitcoin_transactions
    )


def main() -> None:
    
    jobs = [
        bitcoin_transactions
    ]
    
    for job in jobs:
        job.run_etl()


if __name__ == "__main__":
    main()