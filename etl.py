import logging 
import os

from scripts import ( 
    version_two
    )


def main() -> None:
    

    jobs = [
        version_two
    ]
    
    for job in jobs:
        job.run_etl()


if __name__ == "__main__":
    main()