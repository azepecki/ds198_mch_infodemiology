import os
from os.path import join, dirname
import requests
import json
import datetime
import csv
import simulate_keywords

"""
Receives relative_search_volumes and initial search query as inputs. 
Our function then makes a search using Google's API, parses through the first ten sites in the results for each search and writes the appropriate attributes to a csv file in OUTPUT/SEARCH. 
"""
def main(relative_search_volumes, initial_search_query):
    code = "US"
    head, tail = os.path.split(dirname(__file__))
    params = {
        'url' : 'www.googleapis.com/customsearch/v1',
        'cx' : "007577961584064119408%3A0r-ks0pzwfw",
        'key' : "AIzaSyCwrSmxtYMKbIktk0fHRFAGOfsl1t0hMSI",
    }
    output_dir = os.getcwd() + "/output/search/" + code
    simulate_keywords.Simulation.mkdir_p(output_dir)
    name = output_dir + "/" + datetime.datetime.now().strftime('%Y-%m-%d-%H:%M') + ".csv"
    
    with open(name, "w") as csvfile:
        field_names = ["initial_search_query", "query", "position", "link", "displayLink", "site_probability"]
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        for q in relative_search_volumes:
            params['q'] = list(q.keys())[0]
            try:
                text = u'https://{url}?q={q}&cx={cx}&key={key}'.format(**params)
                reponse = requests.get(text.encode("utf8"))
                data = json.loads(reponse.text)
            except requests.exceptions.SSLError as e:
                print(e)
                pass 

            try:
                if data["error"]["code"] == 403:
                    #do some error correcting
                    raise Exception("Over our search quota for today.")
            except:
                index = 1
                try: 
                    for item in data["items"]:
                        probability = return_site_probability(q, str(index))
                        writer.writerow({
                            "initial_search_query": initial_search_query, 
                            "query": list(q.keys())[0], 
                            "position": str(index), 
                            "link": item["link"], 
                            "displayLink": item["displayLink"], 
                            "site_probability": probability
                        })
                        index += 1
                    index = 1
                except UnicodeEncodeError as e:
                    print(e)
                    continue
                except KeyError:
                    continue


"""
Calculate the site probability for a particular site
"""
def return_site_probability(relative_search_volume, position ): 
    query = list(relative_search_volume.keys())[0]
    probability = list(relative_search_volume.values())[0]
    return probability * get_probability(position)


"""
Site probabilities are determined by Chitika study
For reference, please see methodology paper
"""
def get_probability(position): 
    if position == "1": 
        return 0.35
    elif position == "2":
        return 0.20
    elif position == "3":
        return 0.15
    elif position == "4":
        return 0.08
    elif position == "5":
        return 0.07
    elif position == "6":
        return 0.05
    elif position == "7": 
        return 0.04
    elif position == "8":
        return 0.03
    elif position == "9": 
        return 0.02
    else:
        return 0.01
  
