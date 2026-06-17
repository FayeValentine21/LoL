#Import Libraries
import time, requests, json, os, io
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

#Define functions
def safe_request(url, headers):
    '''request reponse, wait rate limits or breaks'''
    while True:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                time.sleep(0.1)
                return response
            elif response.status_code == 429:
                time.sleep(125)
            else:
                response.raise_for_status()

def send_to_databricks(workspace_client, catalog, schema, volume, folder, filename, data):
    '''choose where to save data in databricks and load it'''
    path = f"/Volumes/{catalog}/{schema}/{volume}/{folder}/{filename}.json"
    workspace_client.files.upload(path, io.BytesIO(json.dumps(data).encode()))   

def get_players(queue, tier, division, headers):
    '''Get json list of players in specified queue, tier, division'''
    url = f"https://euw1.api.riotgames.com/lol/league-exp/v4/entries/{queue}/{tier}/{division}?page=1"
    request = safe_request(url, headers)
    return request.json()

def get_matches_id(queue_id, puuids, headers):
    '''get json set of matches_id from a list of puuids'''
    matches = set()
    for puuid in puuids:
        url = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?queue={queue_id}&type=ranked&start=0&count=100"
        request = safe_request(url, headers)
        matches.update(request.json())
    return matches

def get_processed_matches(workspace_client, catalog, schema, volume, folder):
    '''get processed matches already in databricks'''
    matches = set()
    path = f"/Volumes/{catalog}/{schema}/{volume}/{folder}"
    try:
        for file in workspace_client.files.list_directory_contents(path):
            file_name = file.path.split("/")[-1].split("_", 1)[-1].replace(".json", "")
            matches.add(file_name)
    except Exception:
        pass
    return matches

def fetch_and_upload_matches(matches, headers, workspace_client, catalog, schema, volume):
    '''fetch every match of a set to return and upload in databricks both the match and timeline data'''
    for match in matches:
        url_match=f"https://europe.api.riotgames.com/lol/match/v5/matches/{match}"
        url_timeline=f"https://europe.api.riotgames.com/lol/match/v5/matches/{match}/timeline"
        match_data = safe_request(url_match, headers).json()
        timeline_data = safe_request(url_timeline, headers).json()
        send_to_databricks(workspace_client,catalog,schema,volume,"matches",f"match_{match}",match_data)
        send_to_databricks(workspace_client,catalog,schema,volume,"timelines",f"timeline_{match}",timeline_data)      

def main():
    #Get APIs from env
    load_dotenv()
    api_key = os.getenv("RIOT_API_KEY")
    headers = {"X-Riot-Token": api_key}
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    workspace_client = WorkspaceClient(host=host, token=token)

    #Configure variables
    queue = "RANKED_SOLO_5x5"
    tier = "CHALLENGER"
    division = "I"
    queue_id = 420
    catalog = "workspace"
    schema = "bronze"
    volume = "raw_data"

    #Pipeline
    players = get_players(queue, tier, division, headers)
    puuids = [player["puuid"] for player in players]
    matches = get_matches_id(queue_id,puuids,headers)
    old_matches = get_processed_matches(workspace_client, catalog, schema, volume, "matches") & get_processed_matches(workspace_client, catalog, schema, volume, "timelines")
    new_matches = matches - old_matches
    fetch_and_upload_matches(new_matches, headers, workspace_client, catalog, schema, volume)

if __name__ == "__main__":
    main()