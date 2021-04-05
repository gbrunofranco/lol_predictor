import random
import arrow
import cassiopeia as cass
from sortedcontainers import SortedList
from config import *
from cassiopeia.core import Summoner, MatchHistory, Match
from cassiopeia import Queue, Patch, Champion
from cassiopeia.core.match import Participant
import os


cass.set_riot_api_key(API_KEY)
cass.set_default_region(REGION)

match_fields = {
    "dynamic": [
        "id",
        "duration",
        "is_remake",
    ],
    "static": {
        "patch": PATCH,
        "region": REGION
    },
    "team": ["baron_kills", "dragon_kills", "inhibitor_kills"],
    "gamestate":["first_baron", "first_blood", "first_inhibitor", "first_tower", "first_rift_herald", "win"],
    "bans": ["ban_B1", "ban_B2", "ban_B3", "ban_B4", "ban_B5", "ban_R1", "ban_R2", "ban_R3", "ban_R4", "ban_R5"],
    "champions": ["pick_B1", "pick_B2", "pick_B3", "pick_B4", "pick_B5", "pick_R1", "pick_R2", "pick_R3", "pick_R4", "pick_R5"],
}

def setup_files():
    '''
    Setup function that creates the missing files
    '''


    if not os.path.exists(MATCHES_FILE):
        with open(MATCHES_FILE, 'w') as matches_file:
            for field in match_fields["dynamic"] + list(match_fields["static"].keys()) + match_fields["bans"] + match_fields["champions"] + match_fields["gamestate"]:
                print(field, file=matches_file, sep='\t', end='\t')

            print(*[team+field for team in ["blue_","red_"] for field in match_fields["team"] ], file=matches_file, sep='\t', end='\n')

            


def handle_print(match: Match):
    '''
    Handles the printing of a match

            Parameters:
                    match (Match): the match we want to output to the configured .tsv file
    '''


    with open(MATCHES_FILE, 'a') as matches_file:
        print(*[getattr(match, field) for field in match_fields["dynamic"]], file=matches_file, sep='\t', end='\t')
        print(*[value for value in match_fields["static"].values()], file=matches_file, sep='\t', end='\t')
        print(*[banned_champ.id if type(banned_champ) == Champion else -1 for banned_champ in match.blue_team.bans + match.red_team.bans], file=matches_file, sep='\t', end='\t')
        print(*[participant.champion.id for participant in match.blue_team.participants + match.red_team.participants], file=matches_file, sep='\t', end='\t')
        print(*[0 if getattr(match.blue_team, field) == True else 1 for field in match_fields["gamestate"]], file=matches_file, sep='\t', end='\t')
        print(*[getattr(match.blue_team, field) for field in match_fields["team"]], file=matches_file, sep='\t', end='\t')
        print(*[getattr(match.red_team, field) for field in match_fields["team"]], file=matches_file, sep='\t', end='\n')
        

# heavily inspired by: https://github.com/meraki-analytics/cassiopeia/issues/359#issuecomment-787516878
def filter_match_history(summoner: Summoner, patch: Patch) -> MatchHistory:
    '''
    Filters the match history of a summoner to solo q matches and the specified patch

            Parameters:
                    summoner (Summoner): the summoner we want to filter the match history of
                    patch (Patch): the patch we want the filtered matches to be of
            
            Returns:
                match_history (MatchHistory): the filtered match history
    '''


    end_time: arrow.arrow.Arrow = patch.end
    if end_time is None:
        end_time = arrow.now()
    match_history: MatchHistory = MatchHistory(summoner=summoner, queues={Queue.ranked_solo_fives}, begin_time=patch.start, end_time=end_time)
    return match_history


def collect_matches():
    '''
    Collects matches and summoners id in order to feed handle_prints() matches. Discovers new summoners in the games of a default summoner
    '''


    summoner: Summoner = Summoner(name=INIT_SUMMONER, region=REGION)
    patch: Patch = Patch.from_str(PATCH, region=REGION)

    unpulled_summoner_ids: SortedList[str] = SortedList([summoner.id])
    pulled_summoner_ids: SortedList[str] = SortedList()

    unpulled_match_ids: SortedList[str] = SortedList()
    pulled_match_ids: SortedList[str] = SortedList()
    counter = 0
    while unpulled_summoner_ids and counter < 10000:
        # Get a random summoner from our list of unpulled summoners and pull their match history
        new_summoner_id: str = random.choice(unpulled_summoner_ids)
        new_summoner: Summoner = Summoner(id=new_summoner_id, region=REGION)
        matches: MatchHistory = filter_match_history(new_summoner, patch)
        unpulled_match_ids.update([match.id for match in matches])
        unpulled_summoner_ids.remove(new_summoner_id)
        pulled_summoner_ids.add(new_summoner_id)

        [handle_print(match) for match in matches]
        while unpulled_match_ids and counter < 10000:
            # Get a random match from our list of matches
            new_match_id: str = random.choice(unpulled_match_ids)
            new_match: Match = Match(id=new_match_id, region=REGION)
            for participant in new_match.participants:
                participant: Participant
                if participant.summoner.id not in pulled_summoner_ids and participant.summoner.id not in unpulled_summoner_ids:
                    unpulled_summoner_ids.add(participant.summoner.id)
            # The above lines will trigger the match to load its data by iterating over all the participants.
            # If you have a database in your datapipeline, the match will automatically be stored in it.
            unpulled_match_ids.remove(new_match_id)
            pulled_match_ids.add(new_match_id)
            counter = counter +1




if __name__ == "__main__":
    setup_files()
    collect_matches()