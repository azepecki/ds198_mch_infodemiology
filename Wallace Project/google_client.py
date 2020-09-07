import sys
import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
import simulate_keywords
import logging
import time
import copy

class GoogleClient(object):

    def __init__(self, trends_server, trends_version):
        logging.basicConfig(filename=simulate_keywords.Simulation.LOG_FILE,level=logging.INFO)
        self.trends_version = trends_version
        self.trends_server = trends_server
        self.service = self.build_service()

        
    """
    Builds service to our trends api.
    """
    def build_service(self):
        discovery_url = self.trends_server + '/discovery/v1/apis/trends/' + self.trends_version + '/rest'
        trends_developer_key = os.environ.get('TRENDS_DEVELOPER_KEY')
        return build('trends', self.trends_version, developerKey=trends_developer_key, discoveryServiceUrl=discovery_url)

    
    """
    Helper function to parse geolocation to determine what "level" of geolocation
    """
    def _parse_geoLocation(self, geoLocation): 
        split_loc = geoLocation.split("-")
        # If this is at the country level
        if len(split_loc) == 1: 
            return "country"
        # If this is at the state level
        elif len(split_loc) == 2: 
            return "region"
        # If this is at the dma level
        elif len(split_loc) == 3: 
            return "dma"
        else: 
            logging.error("Error: geoLocation not valid!")
            raise

            
    """
     Take a TERM and a GEOLOCATION and a STARTDATE, ENDDATE, determines the top queries. 
    """
    def find_queries(self, word, geoLocation, startDate, endDate):
        returned_queries = []
        returned_values = []
  
        try:
            response = self.service.getTopQueries(
                term=word, restrictions_geo=geoLocation, restrictions_startDate=startDate, restrictions_endDate=endDate
            ).execute()
            if response == {}:
                    return [],[]
            queries = response["item"]
            logging.info(queries)
            # We get all queries, not just those with value >= 70
            # To collect most relevant (>=70) search queries, implement logic in for loop below 
            for q in queries: 
                returned_queries.append(q["title"])
                returned_values.append(q["value"])
                
        except HttpError as e:
            error_content = json.loads(e.content)
            code = error_content["error"]["code"]
            if code == 400: 
                # Not enough data to give us an accurate reading
                logging.info("Not enough data returned by Google; cannot get top queries")
                return [],[]
            elif code == 429: 
                # Our trends rate has been exceeded, wait for two seconds and continue.
                logging.info("Our rate has been exceeded, we will wait for two seconds and continue.")
                time.sleep(2)
                return self.find_queries(word, geoLocation, startDate, endDate)
            elif (code // 100) == 5:
                logging.error("An internal server error occured at the Google service, process and continue.")
                logging.error(e.content)
                time.sleep(3)
                return self.find_queries(word, geoLocation, startDate, endDate)
            else:
                pass
        return returned_queries,returned_values


    """
    Take a TERM and a GEOLOCATION and a STARTDATE, ENDDATE, determines the top topics. 
    """
    def find_topics(self, term, geoLocation, startDate, endDate):
        if geoLocation == None:
            return term
        topics = set()
        try: 
            response = self.service.getTopTopics(
                term=term, restrictions_geo=geoLocation, restrictions_startDate=startDate, restrictions_endDate=endDate
            ).execute()
            items = response["item"]
            top_topic = items[0]["mid"]
            if top_topic not in topics: 
                topics.add(top_topic)
        except HttpError as e:
                error_content = json.loads(e.content)
                code = error_content["error"]["code"]
                if code == 404:
                    logging.info("Not enough info for this word")
                    return []
                elif code == 429:
                    logging.info("Our rate has been exceeded, wait and continue")
                    time.sleep(2)
                    pass
                elif (code // 100) == 5:
                    logging.error("Our google service has an internal server error, process and try again")
                    time.sleep(1)
                    pass
                else:
                    logging.error("an unknown error appeared")
                    logging.error(e)   
        return topics
    
    
    """
    Returns call to getTimelinesForHealth with appropriate parameters based on geoLocation & loc_type
    """
    def _get_service_call_by_loc_type(self, loc_type, geoLocation, terms, startDate, endDate):
        if loc_type == "country":
            geo = geoLocation['code']
            health_value = self.service.getTimelinesForHealth(
                terms=terms,
                time_startDate=startDate,
                time_endDate=endDate,
                geoRestriction_country=geo
            )
            
        elif loc_type == "region": 
            geo = geoLocation['code']
            health_value = self.service.getTimelinesForHealth(
                terms=terms,
                time_startDate=startDate,
                time_endDate=endDate,
                geoRestriction_region=geo
            )
        else:
            geo = geoLocation['code'].split("-")[2]
            health_value = self.service.getTimelinesForHealth(
                terms=terms,
                time_startDate=startDate,
                time_endDate=endDate,
                geoRestriction_dma=geo
            )
        logging.info(health_value)
        return health_value
    
        
    """
    Average points for each term in terms
    """
    def _average(self, values):
        terms = values["lines"]
        term_aggregate = []
        total_agg = 0
        for term in terms:
            new_dict = dict()
            points = term["points"]
            index = 1
            agg = 0
            for point in points:
                agg += point['value']
                index += 1
            average_points = float(agg)
            total_agg += average_points
            new_dict[term["term"]] = average_points
            term_aggregate.append(copy.deepcopy(new_dict))
        ret_values = []
        logging.info(term_aggregate)
        for dic in term_aggregate:
            new_dict = {}
            key = list(dic.keys())[0]
            value = list(dic.values())[0]
            average_value = value / float(total_agg)
            new_dict[key] = average_value
            ret_values.append(copy.deepcopy(new_dict))
        return ret_values

    
    """
    Get normalized relative search volumes using getTimelinesForHealth google api call 
    """
    def get_timelines_for_health(self, terms, geoLocation, startDate, endDate): 
        loc_type = self._parse_geoLocation(geoLocation['code'])
        points = dict()
        # getTimelinesForHealth can only take 30 items at a time 
        # To get around this, we gather the frequencies for terms multiple times and compute the average frequency for the terms
        if len(terms) <= 30:
            health_value = self._get_service_call_by_loc_type(loc_type, geoLocation, terms, startDate, endDate)
            try: 
                points = health_value.execute()
                return self._average(points) 
            except HttpError as e:
                error_content = json.loads(e.content)
                code = error_content["error"]["code"]
                if code == 404:
                    logging.info("Not enough info for this word")
                    return []
                elif code == 429:
                    logging.info("Our rate has been exceeded, wait and continue")
                    time.sleep(2)
                    pass
                elif (code // 100) == 5:
                    logging.error("Our google service has an internal server error, process and try again")
                    time.sleep(1)
                    pass
                else:
                    logging.error("an unknown error appeared")
                    logging.error(e) 
        else:
            bottom, top, block = 0, 1, 30
            bottom_index = bottom * block
            top_index = top * block
            points = dict()
            points['lines'] = []
            while top_index < len(terms):
                request_terms = terms[bottom_index:top_index]
                health_value = self._get_service_call_by_loc_type(loc_type, geoLocation, request_terms, startDate, endDate)
                try: 
                    points['lines'].extend(health_value.execute()['lines'])
                except HttpError as e:
                    error_content = json.loads(e.content)
                    code = error_content["error"]["code"]
                    if code == 404:
                        logging.info("Not enough info for this word")
                        return []
                    elif code == 429:
                        logging.info("Our rate has been exceeded, wait and continue")
                        time.sleep(2)
                        pass
                    elif (code // 100) == 5:
                        logging.error(error_content)
                        logging.error("Our google service has an internal server error, process and try again")
                        time.sleep(1)
                        pass
                    else:
                        logging.error("an unknown error appeared")
                        logging.error(e) 

                bottom_index += block
                top_index += block
          
            top_index = len(terms)
            request_terms = terms[bottom_index:top_index]
            
            health_value = self._get_service_call_by_loc_type(loc_type, geoLocation, request_terms, startDate, endDate)
            
            try: 
                points['lines'].extend(health_value.execute()['lines'])
            except HttpError as e:
                error_content = json.loads(e.content)
                code = error_content["error"]["code"]
                if code == 404:
                    logging.info("Not enough info for this word")
                    return []
                elif code == 429:
                    logging.info("Our rate has been exceeded, wait and continue")
                    time.sleep(2)
                    pass
                elif (code // 100) == 5:
                    logging.error(error_content)
                    logging.error("Our google service has an internal server error, process and try again")
                    time.sleep(1)
                    pass
                else:
                    logging.error("an unknown error appeared")
                    logging.error(e) 
            return self._average(points)     

  