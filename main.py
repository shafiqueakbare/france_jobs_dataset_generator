
from indeed import *

if __name__ == '__main__':

    jobs_titles = get_list_of_jobs_titles_from_open_data()

    if jobs_titles is not None:
        if create_jobs_dataset_from_indeed(jobs_titles):
            print("Dataset correctly generated")
