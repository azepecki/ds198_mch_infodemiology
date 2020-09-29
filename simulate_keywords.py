from os.path import join, dirname
import os
import logging
import datetime
import csv
import errno
import time
from google_client import GoogleClient


class Simulation(object):
    LOG_FILE = "log/simulation.log" # Logging file output
    TRENDS_SERVER = "https://www.googleapis.com"
    LOCATIONS_FILE = "simulation_locations.csv" # List of locations to run the simulation for
    TRENDS_VERSION = "v1beta"
    K = 1

    def __init__(self, initial_search_term, geoLocation, startDateTrends, endDateTrends, startDateTimelines, endDateTimelines):
        self.initial_search_term = initial_search_term
        self.geoLocation = geoLocation
        self.startDateTrends = startDateTrends
        self.endDateTrends = endDateTrends
        self.startDateTimelines = startDateTimelines
        self.endDateTimelines = endDateTimelines
        self.google_client = GoogleClient(Simulation.TRENDS_SERVER, Simulation.TRENDS_VERSION)
        self.topics = []
        self.initial_queries = []
        self.relative_search_volumes = []

    
    """
    Functionality of mkdir -p in unix command line system
    """
    @staticmethod
    def mkdir_p(path):
        try: 
            os.makedirs(path)
        except OSError as exc: 
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise
           
    """
    Create csv files recording the contents of this simulation
    """
    def generate_simulation_csvs(self):
        self.generate_queries_csv()
        self.generate_relative_search_volumes_csv()
                    
    
    def generate_queries_csv(self): 
        output_dir = os.getcwd() + "/output/keywords/" + self.geoLocation['code'] + "/top_queries"
        Simulation.mkdir_p(output_dir)
        name = output_dir + "/" + datetime.datetime.now().strftime('%Y-%m-%d-%H:%M') + ".csv"
        with open(name, "w") as csvfile: 
            field_names = ["initial search term", "startDate", "endDate", "query", "value", "level"]
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for item in self.initial_queries: 
                try: 
                    writer.writerow({
                        "initial search term": self.initial_search_term,
                        "startDate": self.startDateTrends,
                        "endDate": self.endDateTrends,
                        "query": item['query'], 
                        "value": item['value'], 
                        "level": item['level']
                    })
                except UnicodeEncodeError as e: 
                    logging.error("Unicode Error: {}".format(e.object[e.start:e.end]))
                    continue
                    
    
    def generate_relative_search_volumes_csv(self): 
        output_dir = os.getcwd() + "/output/keywords/" + self.geoLocation['code'] + "/relative_search_volume"
        Simulation.mkdir_p(output_dir)
        name = output_dir + "/" + datetime.datetime.now().strftime('%Y-%m-%d-%H:%M') + ".csv"
        with open(name, "w") as csvfile: 
            field_names = ["term", "relative_search_volume"]
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            writer.writeheader()
            for item in self.relative_search_volumes: 
                try: 
                    writer.writerow({
                        "term": list(item.keys())[0],
                        "relative_search_volume": list(item.values())[0] 
                    })
                except UnicodeEncodeError as e: 
                    logging.error("Unicode Error: {}".format(e.object[e.start:e.end]))
                    continue
    
    
    """
    Sets the Topics for a given simulation.
    """
    def get_topics(self):
        self.topics = self.google_client.find_topics(
            self.initial_search_term, self.geoLocation['code'], self.startDateTrends, self.endDateTrends
        )

    
    """
    Obtains the top queries for a given simulation
    """
    def get_queries(self):
        queries, values = self.google_client.find_queries(
            self.initial_search_term, self.geoLocation['code'], self.startDateTrends, self.endDateTrends
        )
        for query, value in zip(queries, values): 
            item = {"query": query, "value": value, "level": 1, "follow_up_terms": []}
            self.initial_queries.append(item)


    """
    Function that returns the keywords generated from a seed set. 
    Change the GEOLOCATION to specify the area you wish to cover. 
    Get the developer API from a team member and assign as an env variable. 
    There are a few places to look out for exceptions in this function. 
    If the keyword_number argument is set to an invalid number, a ValueError is raised in our get_case_words function. 
    """
    def generate_keywords(self):
        logging.info(self.geoLocation)
        logging.info("Starting simulation with Trends for area: " + self.geoLocation['description'])
        try:
            self.get_topics()
            self.get_queries()
        except ValueError as e:
            logging.error("Could not evaluate seed set")
            raise e
 
        queries_no_duplicates = set([x['query'] for x in self.initial_queries])
    
        for item in self.initial_queries:
            follow_up_queries, follow_up_values = self.google_client.find_queries(
                item['query'], self.geoLocation, self.startDateTrends, self.endDateTrends
            )
            for query, value in zip(follow_up_queries, follow_up_values): 
                if query not in queries_no_duplicates: 
                    logging.info("Adding {} to our seed set.".format(query))
                    new_item = {"query": query, "value": value, "level": 2, "follow_up_terms": []}
                    item['follow_up_terms'].append(new_item)
                    queries_no_duplicates.add(query)
            # Then, we look at the next level of queries
            # To look beyond three levels, modify this function
            for second_level_item in item['follow_up_terms']:
                follow_up_queries, follow_up_values = self.google_client.find_queries(
                    second_level_item['query'], self.geoLocation, self.startDateTrends, self.endDateTrends
                )
                for query, value in zip(follow_up_queries, follow_up_values): 
                    if query not in queries_no_duplicates: 
                        logging.info("Adding {} to our seed set.".format(query))
                        new_item = {"query": query, "value": value, "level": 3, "follow_up_terms": []}
                        second_level_item['follow_up_terms'].append(new_item)
                        queries_no_duplicates.add(query)
                        
    
    def get_relative_search_volumes(self): 
        logging.info("Starting simulation with health trends")
        # You could also override terms to use your masterlist so that you are working with the same list of terms for all locs
        terms = [query['query'] for query in self.initial_queries]
        relative_search_volumes = self.google_client.get_timelines_for_health(
            terms, self.geoLocation, self.startDateTimelines, self.endDateTimelines
        )
        self.relative_search_volumes = relative_search_volumes
    