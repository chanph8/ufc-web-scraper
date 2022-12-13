import os

from signal import signal, SIGINT
from flask import Flask, render_template


import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
# import requests

from urllib.request import urlopen
from urllib.error import HTTPError
from urllib.error import URLError
import re
from google.cloud import storage


import atexit
from flask_apscheduler import APScheduler
from datetime import datetime

#from google.cloud import logging

def removeLocal_and_download(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    from os.path import exists
    import traceback
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # source_blob_name = "storage-object-name"

    # The path to which the file should be downloaded
    # destination_file_name = "local/path/to/file"

    if exists(destination_file_name):
        os.remove(destination_file_name)

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    try:
        # Construct a client side representation of a blob.
        # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
        # any content from Google Cloud Storage. As we don't need additional data,
        # using `Bucket.blob` is preferred here.
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

        print(
            "Downloaded storage object {} from bucket {} to local file {}.".format(
                source_blob_name, bucket_name, destination_file_name
            )
        )
    except:
        os.remove(destination_file_name)
        print("Failed to get {} from bucket store".format(source_blob_name))


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    bucket.acl.reload()
    bucket.default_object_acl.user("all").grant_read()

    blob.make_public()

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )




def get_bs(url):
    """Get the BeautifulSoup html object of a url"""
    try:
        html = urlopen(url)
        bs = BeautifulSoup(html.read(), 'html.parser')
    except HTTPError as e:
        print(e)
        
    return bs


def find_all_urls(bs, regex_condition = '.+/fight-details/.+'):
    """Return a set of unique urls in a BeautifulSoup html object with a regex search condition"""
    urls = set()
    
    find_all_results = bs.find_all('a', href = re.compile(regex_condition))

    if len(find_all_results) != 0:
        for u in find_all_results:
            if 'href' in u.attrs:
                url = u.attrs['href']
                urls.add(url)
    else:
        return []
            
    return urls



def get_match_details(match_url):
    """Parse /fight-details/ pages as BeautifulSoup objects"""

    # Set up empty lists that will contain all of the features of each player within /fight-details/.
    p1 = []
    p2 = []

    # Web request
    try:
        html = urlopen(match_url)
        bs = BeautifulSoup(html.read(), 'html.parser')
    except HTTPError as e:
        print(e)
        
    # Figher names    
    player_names_and_outcomes = bs.find_all('div', {'class': {'b-fight-details__person'}})
    p1_name = player_names_and_outcomes[0].find('a', {'class': 'b-link b-fight-details__person-link'}).text
    p2_name = player_names_and_outcomes[1].find('a', {'class': 'b-link b-fight-details__person-link'}).text

    p1.append(p1_name)
    p2.append(p2_name)

    # Weight class
    weight_class = bs.find('i', {'class': 'b-fight-details__fight-title'}).text.strip()[:-5]

    p1.append(weight_class)
    p2.append(weight_class)

    if player_names_and_outcomes[0].find('i', {'class': 'b-fight-details__person-status b-fight-details__person-status_style_green'}):
        p1_win = 1
        p2_win = 0
    else:
        p2_win = 1
        p1_win = 0

    p1.append(p1_win)
    p2.append(p2_win)

    # Get the match length. Matches can be 15 or 25 minutes long, affects the ctrl variable.
    time = bs.find( 'i', 
                    {'class': 'b-fight-details__label'}, 
                    text=re.compile(r'(.+Time format.+)'))\
              .next_sibling.strip()

    # Check if the time format is a 5-round fight (25 minutes). Otherwise, it's 3-round.
    pattern = re.compile("5 Rnd")
    if pattern.search(time):
        duration = 25
    else:
        duration = 15

    p1.append(duration)
    p2.append(duration)

    # Collect all relevant stats from /fight-details/.

    tables = bs.find_all('tbody', {'class': 'b-fight-details__table-body'})

    # The first table is the TOTALs table, tbody with class: b-fight-details__table-body
    # Within the table the stats are selected via p with class: b-fight-details__table-text
    # The first two stats are the names which can be skipped.

    if len(tables) > 0:
        totals_table = tables[0].find_all('p', {'class': 'b-fight-details__table-text'})

        for i in range(2, 20): # skip first two records in table which are the players' names

            stat = totals_table[i].text.strip()

            # The stats are in order of player 1, player 2, player 1, player 2. player 1 only has even values.
            if i % 2 != 1:
                if stat[-1] == '%': # if stat is percent as string
                    stat = int(stat[:-1])/100 # convert to numeric
                if stat == '---' or stat == '--':
                    stat = 0
                p1.append(stat)
            else:
                if stat[-1] == '%': # if stat is percent as string
                    stat = int(stat[:-1])/100 # convert to numeric
                if stat == '---' or stat == '--':
                    stat = 0
                p2.append(stat)

        # Significant Strikes Table

        sig_strikes_table = tables[2].find_all('p', {'class': 'b-fight-details__table-text'})

        # Skip first several records which are the players' names and duplicate stats
        for i in range(6, 18):

            stat = sig_strikes_table[i].text.strip()

            if i % 2 != 1:
                p1.append(stat)
            else:
                p2.append(stat)

        # When there is no stat, it shows as '---'. Convert to 0.
        for i in range(len(p1)):
            if p1[i] == '---' or p1[i] == '--':
                p1[i] = 0
            else:
                continue
        for i in range(len(p2)):
            if p2[i] == '---' or p2[i] == '--':
                p2[i] = 0
            else:
                continue

        player_urls = list(find_all_urls(tables[0], regex_condition='.+/fighter-details/.+'))

        p1.append(player_urls[0])
        p2.append(player_urls[1])

        p1_name = p1[0] # player 1 name
        p2_name = p2[0] # player 2 name

        # Create a dictionary of match details

        match_details = {} # Dictionary of match stats

        match_details['player_1_name'] = p1[0].strip()
        match_details['player_2_name'] = p2[0].strip()

        match_details['weight_class'] = p1[1] # weight class of players

        match_details['win'] = int( p1[2] ) # boolean of whether player 1 won

        match_details['duration'] = int( p1[3] ) # duration of match in minutes

        match_details['kd'] = int( p1[4] ) # knock downs
        match_details['o_kd'] = int( p2[4] ) # opponent knock downs

        match_details['ss_hit'] = int( p1[5].split()[0] ) # significant strikes landed
        match_details['ss_att'] = int( p1[5].split()[2] ) # significant strikes attempted
        match_details['o_ss_hit'] = int( p2[5].split()[0] ) # opponent significant strikes landed
        match_details['o_ss_att'] = int( p2[5].split()[2] ) # opponent significant strikes attempted

        match_details['ss_perc']  = float( p1[6] ) # significant strike percentage
        match_details['o_ss_perc'] = float( p2[6] ) # opponent significant strike percentage

        match_details['s_hit'] = int( p1[7].split()[0] ) # strikes landed
        match_details['s_att'] = int( p1[7].split()[2] ) # strikes attempted
        match_details['o_s_att'] = int( p2[7].split()[2] ) # opponent strikes attempted
        match_details['o_s_hit'] = int( p2[7].split()[0] ) # opponent strikes landed

        match_details['td_hit'] = int( p1[8].split()[0] ) # takedowns landed
        match_details['td_att'] = int( p1[8].split()[2] ) # takedowns attempted
        match_details['o_td_hit'] = int( p2[8].split()[0] ) # opponent takedowns landed
        match_details['o_td_att'] = int( p2[8].split()[2] ) # opponent takedowns attempted

        match_details['td_perc']  = float ( p1[9] ) # takedown percentage
        match_details['o_td_perc'] = float( p2[9] ) # opponent takedown percentage

        match_details['sub_att'] = int( p1[10] ) # submission attempts
        match_details['o_sub_att'] = int( p2[10] ) # opponent submission attempts

        match_details['rev'] = int( p1[11] ) # reversals to a takedown
        match_details['o_rev'] = int( p2[11] ) # opponent reversals to a takedown 

        try: # if ctrl is a timestamp, convert to seconds. Otherwise it is 0 and we can ignore the error
            match_details['ctrl'] = int( p1[12].split(':')[0] )  * 60 + int(p1[12].split(':')[1]) # control in seconds
            match_details['o_ctrl'] = int( p2[12].split(':')[0] ) * 60 + int(p2[12].split(':')[1]) # opponent control in seconds
        except:
            match_details['ctrl'] = 0
            match_details['o_ctrl'] = 0

        match_details['ss_head_hit'] = int( p1[13].split()[0] ) # significant strikes to the head landed
        match_details['ss_head_att'] = int( p1[13].split()[2] ) # significant strikes to the head attempted
        match_details['o_ss_head_hit'] = int( p2[13].split()[0] ) # opponent significant strikes to the head landed
        match_details['o_ss_head_att'] = int( p2[13].split()[2] ) # opponent significant strikes to the head attempted

        match_details['ss_body_hit'] = int( p1[14].split()[0] ) # significant strikes to the body landed
        match_details['ss_body_att'] = int( p1[14].split()[2] ) # significant strikes to the body attempted
        match_details['o_ss_body_hit'] = int( p2[14].split()[0] ) # opponent significant strikes to the body landed
        match_details['o_ss_body_att'] = int( p2[14].split()[2] ) # opponent significant strikes to the body attempted

        match_details['ss_leg_hit'] = int( p1[15].split()[0] ) # significant strikes to the leg landed
        match_details['ss_leg_att'] = int( p1[15].split()[2] ) # significant strikes to the leg attempted
        match_details['o_ss_leg_hit'] = int( p2[15].split()[0] ) # opponent significant strikes to the leg landed
        match_details['o_ss_leg_att'] = int( p2[15].split()[2] ) # opponent significant strikes to the leg attempted

        match_details['ss_dist_hit'] = int( p1[16].split()[0] ) # significant strikes at a distance landed
        match_details['ss_dist_att'] = int( p1[16].split()[2] ) # significant strikes at a distance attempted
        match_details['o_ss_dist_hit'] = int( p2[16].split()[0] ) # opponent significant strikes at a distance landed
        match_details['o_ss_dist_att'] = int( p2[16].split()[2] ) # opponent significant strikes at a distance attempted

        match_details['ss_clinch_hit'] = int( p1[17].split()[0] ) # significant strikes in the clinch landed
        match_details['ss_clinch_att'] = int( p1[17].split()[2] ) # significant strikes in the clinch attempted
        match_details['o_ss_clinch_hit'] = int( p2[17].split()[0] ) # opponent significant strikes in the clinch landed
        match_details['o_ss_clinch_att'] = int( p2[17].split()[2] ) # opponent significant strikes in the clinch attempted

        match_details['ss_ground_hit'] = int( p1[18].split()[0] ) # significant strikes on the ground landed
        match_details['ss_ground_att'] = int( p1[18].split()[2] ) # significant strikes on the ground attempted
        match_details['o_ss_ground_hit'] = int( p2[18].split()[0] ) # opponent significant strikes on the ground landed
        match_details['o_ss_ground_att'] = int( p2[18].split()[2] ) # opponent significant strikes on the ground attempted

        match_details['player_1_url'] = p1[19] # player 1 /player-details/ url
        match_details['player_2_url'] = p2[19] # player 2 /player-details/ url
        
        match_details['match_url'] = match_url

        return match_details
    
    else:
        print('Failed to get match details.')
        
        match_details = None
        
        return match_details


def get_weight_class(weight):
    if weight <= 115:
        weightclass = 'Strawweight'
    elif weight <= 125:
        weightclass = 'Flyweight'
    elif weight <= 135:
        weightclass = 'Bantamweight'
    elif weight <= 145:
        weightclass = 'Featherweight'
    elif weight <= 155:
        weightclass = 'Lightweight'
    elif weight <= 170:
        weightclass = 'Welterweight'
    elif weight <= 185:
        weightclass = 'Middleweight'
    elif weight <= 205:
        weightclass = 'Light Heavyweight'
    elif weight > 205:
        weightclass = 'Heavyweight'
    else:
        weightclass = 'No weight data'
        
    return weightclass




def get_player_details(player_url):
    """Return list of stats scraped from /player-details/"""
    
    player_details = {}
    
    # Web request
    try:
        html = urlopen(player_url)
        bs = BeautifulSoup(html.read(), 'html.parser')
    except HTTPError as e:
        print(e)
    
    # player url
    player_details['player_url'] = player_url
    
    # player_name
    player_details['player_name'] = bs.find('span', {'class': 'b-content__title-highlight'}).text.strip()
    
    # wins, losses, ties, no-contests, total matches
    player_record = bs.find('span', {'class': 'b-content__title-record'}).text.strip()[8:].split('-')
    player_details['wins'] = int( player_record[0] )

    player_details['losses'] = int( player_record[1] )

    ties_nc = player_record[2] 
    if 'NC' in ties_nc:
        player_details['ties'] = int( ties_nc.split('(')[0] )
        player_details['nc'] = int( ties_nc.split('(')[1].split()[0] )
    else:
        player_details['ties'] = int( ties_nc.split('(')[0] )
        player_details['nc'] = 0
        
    player_details['total_matches'] = player_details['wins'] + \
                                      player_details['losses'] + \
                                      player_details['ties'] + \
                                      player_details['nc']
    
    # Physical statistics
    physical_stats = bs.find_all('li', {'class': 'b-list__box-list-item b-list__box-list-item_type_block'})

    # Using a number of try/except statements because not every player has every stat
    # Height in inches
    try: # not all player have heights
        height_list = physical_stats[0].text.split()[1:]
        height_feet = ''.join(char for char in height_list[0] if char.isdigit())
        height_inches = ''.join(char for char in height_list[1] if char.isdigit())
        player_details['height'] = int(height_feet) * 12 + int(height_inches)
    except:
        player_details['height'] = np.nan
        
    # Weight in lbs
    try:
        player_details['weight'] = int( physical_stats[1].text.split()[1] )
    except:
        player_details['weight'] = np.nan
        
    # Weight class
    try:
        weight_class = get_weight_class(player_details['weight'])
        player_details['weight_class'] = weight_class
    except:
        player_details['weight_class'] = 'No weight data'

    # Reach in inches
    try:
        player_details['reach'] = int( physical_stats[2].text.split()[1][0:2] ) # can only be two digits, nobody is taller than 8.3 ft
    except:
        player_details['reach'] = np.nan

    # Stance (Orthodox = 1, right hand and right leg are forward. Southpaw = 0, left hand and left leg are forward.)
    try:
        player_details['stance'] = physical_stats[3].text.split()[1]
        if player_details['stance'] == 'Orthodox': # boolean allows us to check if two players have the same stance
            player_details['stance'] = 1
        else:
            player_details['stance'] = 0
    except:
        player_details['stance'] = 1

    # Age
    try:
        player_details['age'] = 2022 - int( physical_stats[4].text.split()[-1] )
    except:
        player_details['age'] = np.nan

    # significant strikes landed per minute
    try:
        player_details['ss'] = float( physical_stats[5].text.split()[1] )
    except:
        player_details['ss'] = np.nan

    # percent of significant strikes landed 
    try:
        player_details['ss_acc'] = round( int( physical_stats[6].text.split()[2][:-1] ) / 100, 2)
    except:
        player_details['ss_acc'] = np.nan

    # significant strikes absorbed per minute
    try:
        player_details['ss_abs'] = float ( physical_stats[7].text.split()[1] )
    except:
        player_details['ss_abs'] = np.nan

    # percent of significant strikes defended/blocked
    try:
        player_details['ss_def'] = round( int( physical_stats[8].text.split()[-1][:-1] ) / 100, 2)
    except:
        player_details['ss_def'] = np.nan

    # takedowns landed per 15 minutes
    try:
        player_details['td'] = float( physical_stats[10].text.split()[-1] )
    except:
        player_details['td'] = np.nan

    # percent takedown accuracy
    try:
        player_details['td_acc'] = round( int ( physical_stats[11].text.split()[-1][:-1] ) / 100, 2 )
    except:
        player_details['td_acc'] = np.nan

    # percent takedown defense
    try:
        player_details['td_def'] = round( int ( physical_stats[12].text.split()[-1][:-1] ) / 100, 2 )
    except:
        player_details['td_def'] = np.nan

    # average submission attemtps per 15 minutes
    try:
        player_details['sub'] = float ( physical_stats[13].text.split()[-1] )
    except:
        player_details['sub'] = np.nan

    # Number of wins in the last 3 matches 
    prev = 0
    try:
        previous_matches = bs.find_all('i', {'class': 'b-flag__text'})
        if previous_matches[0].text == 'next': # if the first record shows the next match rather than a previous match
            previous_matches = previous_matches[1:4] # look at the last 3 matches and sum the wins
            for previous_match in previous_matches:
                if previous_match.text == 'win':
                    prev += 1
            player_details['prev'] = prev
            
        else:
            previous_matches = previous_matches[0:3] # change starting point because there is no next match
            for previous_match in previous_matches:
                if previous_match.text == 'win':
                    prev += 1
            player_details['prev'] = prev
    except:
        player_details['previous_matches'] = 0
        
    return player_details
    


def scrape_completed_matches():
    from os.path import exists
    # # # CREATE DATAFRAMES OF COMPLETED MATCHES AND PLAYER DETAILS # # #

    players={}
    #Load processed

    #processed URLs
    removeLocal_and_download('ufc-scraper-data', 'completed_event_urls_processed.csv', './completed_event_urls_processed.csv')
    if exists('./completed_event_urls_processed.csv'):
        completed_event_urls_processed = pd.read_csv('./completed_event_urls_processed.csv', header=None).values.tolist()[0]
    else:
        completed_event_urls_processed = []


    removeLocal_and_download('ufc-scraper-data', 'completed_match_urls_processed.csv', './completed_match_urls_processed.csv')
    if exists('./completed_match_urls_processed.csv'):
        completed_match_urls_processed = pd.read_csv('./completed_match_urls_processed.csv', header=None).values.tolist()[0]
    else:
        completed_match_urls_processed = []

    # removeLocal_and_download('ufc-scraper-data', 'player_urls_processed.csv', './player_urls_processed.csv')
    # if exists('./player_urls_processed.csv'):
    #     player_urls_processed = pd.read_csv('./player_urls_processed.csv', header=None)
    # else:
    #     player_urls_processed = []
    # Not caching players for now
    player_urls_processed = []

    # removeLocal_and_download('ufc-scraper-data', 'players.csv', './players.csv')
    # if exists('./players.csv'):
    #     players_df = pd.read_csv('players.csv')
    # else:
    #     players_df = []


    #completed_matches_df for the ML pipeline
    removeLocal_and_download('ufc-scraper-data', 'completed_matches.csv', './completed_matches.csv')
    if exists('./completed_matches.csv'):
        completed_matches_df = pd.read_csv('completed_matches.csv')
    else:
        completed_matches_df = []

    print("# of existing completed_event_urls_processed: {}".format(len(completed_event_urls_processed)))
    print(completed_event_urls_processed)
    print("# of existing completed_match_urls_processed: {}".format(len(completed_match_urls_processed)))
    #print(completed_match_urls_processed)
    print("# of existing completed_matches_df: {}".format(len(completed_matches_df)))

    # # # CREATE DATAFRAMES OF COMPLETED MATCHES AND PLAYER DETAILS # # #

    # start url: http://ufcstats.com/statistics/events/completed

    # event urls: http://ufcstats.com/event-details/4bf3010fdeea9d93

    # match urls: http://ufcstats.com/fight-details/4bf3010fdeea9d93

    # Completed events: UFC events that happened in the past
    completed_events_bs = get_bs('http://ufcstats.com/statistics/events/completed?page=all')
    completed_events_urls = find_all_urls(completed_events_bs, regex_condition='.+/event-details/.+')
    # pd.Series(list(completed_events_urls)).to_csv('completed_events_urlsx.csv')

    # Completed matches: Matches that happened in the past that were scraped in this run.
    completed_matches = []

    # Each event has several matches
    #for event_url in list(completed_events_urls)[:1]: #for testing only
    for event_url in list(completed_events_urls):
        
        if event_url in completed_event_urls_processed: 
            print('Event already processed:', event_url)
            continue
        
        # Scrape /event-details/
        print('Scraping event url:', event_url)
        event_bs = get_bs(event_url)
        
        # Returns a list ['23', 'January', '2020']
        event_date_list = event_bs.find_all('li', {'class': 'b-list__box-list-item'})[0].text.split()[1:]
        event_date = event_date_list[0] + ' ' + event_date_list[1] + ' ' + event_date_list[2]
        event_month = event_date_list[1]
        event_year = event_date_list[2]
        
        # Compile /fight-details/ urls on /event-details/ page
        print('Finding match urls')
        match_urls = find_all_urls(event_bs)
        
        if len(match_urls) > 0:
            
            print(f'Found {len(match_urls)} matches in the event.')

            # Scrape each /fight-details/ within /event-details/
            for match_url in match_urls:

                if match_url in completed_match_urls_processed:
                    continue

                print(f'Getting match details: {match_url}')

                match_details = get_match_details(match_url)

                if match_details is None:
                    
                    completed_match_urls_processed.append(match_url)
                    
                    continue
                    
                match_details['event_date'] = event_date
                
                match_details['event_month'] = event_month
                
                match_details['event_year'] = event_year
                
                # Get player 1 details from /fighter-details/
                if match_details['player_1_url'] in player_urls_processed:
                    
                    player_1_details = players[ match_details['player_1_url'] ]
                    
                    print(player_1_details['player_name'], 'player details already collected.')
                
                else:

                    player_urls_processed.append(match_details['player_1_url'])

                    player_1_details = get_player_details(match_details['player_1_url'])

                    print('\nSaving player 1 details:', player_1_details['player_name'])

                    players[ player_1_details['player_url'] ] = player_1_details

                # Get player 2 details from /fighter-details/
                if match_details['player_2_url'] in player_urls_processed:
                    
                    player_2_details = players[ match_details['player_2_url'] ]
                    
                    print(player_2_details['player_name'], 'player details already collected.')

                else:
                    
                    player_urls_processed.append(match_details['player_2_url'])

                    player_2_details = get_player_details(match_details['player_2_url'])

                    print('\nSaving player 2 details:', player_2_details['player_name'], '\n')

                    players[ player_2_details['player_url'] ] = player_2_details

                # Create features of the differences between player 1 and player 2's player stats
                players_diffs = {}

                for key, value in player_1_details.items():

                    if type(value) in (float, int):

                        players_diffs[key + '_diff'] = round( player_1_details[key] - player_2_details[key] , 2 )

                # Concatenate the differences in player 1 and 2 with match details for the final set of match features here.

                match_details = {**match_details, **players_diffs}

                # Concatenate the match_details to completed_matches
                completed_matches.append(match_details)

                completed_match_urls_processed.append(match_url)
            
        else: # if a match is not found (None) then skip next time and continue loop

            #completed_match_urls_processed.append(match_url)
            continue
            
        completed_event_urls_processed.append(event_url)
        
    # Add weight classes to completed_matches:

    print("add weight classes to completed matches")
    for i in range(len(completed_matches)):
        
        weight_class = players[ completed_matches[i]['player_1_url']]['weight_class']
        
        completed_matches[i]['weight_class'] = weight_class

    # Completed matches
    completed_matches_to_add_list = [i for i in completed_matches if i is not None]
    completed_matches_to_add_df = pd.DataFrame(completed_matches_to_add_list)

    if len(completed_matches_df) == 0: # if this variable is a blank list rather than a dataframe of existing matches
        completed_matches_df = completed_matches_to_add_df
    else:
        completed_matches_df = pd.concat([completed_matches_df, completed_matches_to_add_df]).drop_duplicates()

    completed_matches_df.loc[ ~ completed_matches_df.match_url.isna()].to_csv('completed_matches_new.csv', index=False)

    print("upload completed_matches to cloud bucket")
    upload_blob('ufc-scraper-data', './completed_matches_new.csv', 'completed_matches.csv')

    import csv
    with open('./completed_event_urls_processed.csv', 'w+', newline='') as f:
        write = csv.writer(f)
        write.writerows([completed_event_urls_processed])
    print('uploading completed_event_urls_processed')
    upload_blob('ufc-scraper-data', './completed_event_urls_processed.csv', 'completed_event_urls_processed.csv')


    with open('./completed_match_urls_processed.csv', 'w+', newline='') as f:
        write = csv.writer(f)
        write.writerows([completed_match_urls_processed])
    print('uploading completed_match_urls_processed')
    upload_blob('ufc-scraper-data', './completed_match_urls_processed.csv', 'completed_match_urls_processed.csv')
    

    # # Update or create a dataframe of players and save to CSV.
    # players_to_add_df = pd.DataFrame(players).T.reset_index().drop('index', axis=1)

    # if len(players_df) == 0:
    #     players_df = players_to_add_df
    # else:
    #     players_df = pd.contact(players_df, players_to_add_df)

    # players_df.to_csv('players_new.csv')
    # upload_blob('ufc-scraper-data', './players_new.csv', 'players.csv')


def scrape_upcoming_matches():
    from os.path import exists
    # # # CREATE DATAFRAME OF UPCOMING MATCHES # # #

    # Upcoming events: The next few UFC events that have been posted
    upcoming_events_bs = get_bs('http://ufcstats.com/statistics/events/upcoming')
    upcoming_events_urls = find_all_urls(upcoming_events_bs, regex_condition='.+/event-details/.+')
    pd.Series(list(upcoming_events_urls)).to_csv('upcoming_event_urls.csv')

    players={}

    #Load processed
    removeLocal_and_download('ufc-scraper-data', 'upcoming_event_urls_processed.csv', './upcoming_event_urls_processed.csv')
    if exists('./upcoming_event_urls_processed.csv'):
        upcoming_event_urls_processed = pd.read_csv('./upcoming_event_urls_processed.csv', header=None)
    else:
        upcoming_event_urls_processed = []


    # Upcoming matches: Matches that are scheduled to happen that were scraped in this run.
    upcoming_matches = []

    # Group player urls into lists of length two (per match), where player_1 is first url and player_2 is second
    def chunks(lst, n):
        """Yield successive n-sized chunks from lst."""

        for i in range(0, len(lst), n):

            yield lst[i:i + n]

    # Scrape upcoming matches page
    upcoming_events_bs = get_bs('http://ufcstats.com/statistics/events/upcoming?page=all')
    upcoming_events_urls = find_all_urls(upcoming_events_bs, regex_condition='.+/event-details/.+')

    for upcoming_event_url in upcoming_events_urls:
        
        if upcoming_event_url in upcoming_event_urls_processed:
            continue

        # Scrape /event-details/
        print('Scraping event url:', upcoming_event_url)
        event_bs = get_bs(upcoming_event_url)

        # Compile /fight-details/ urls on /event-details/ page
        print('Finding match urls')

        player_url_tags = event_bs.find_all('a', {'href': re.compile('.+/fighter-details/.+')})

        if len(player_url_tags) == 0:
            print('Error. No players found on event page.')
            pass

        player_urls = []

        for i in range(len(player_url_tags)): # extract the urls from the tags and save to player_urls

            player_urls.append(player_url_tags[i]['href'])

        matches = list(chunks(player_urls, 2)) # create a list of lists of length 2, representing each match

        for player_url in matches:

            if player_url[0] not in players:

                player_1_details = get_player_details(player_url[0])

                players[ player_1_details['player_url'] ] = player_1_details
                
            else:
                
                player_1_details = players[ player_url[0] ]

            if player_url[1] not in players:

                player_2_details = get_player_details(player_url[1])

                players[ player_2_details['player_url'] ] = player_2_details
                
            else:
                
                player_2_details = players[ player_url[1] ]

            players_diffs = {}

            players_diffs['player_1_name'] = player_1_details['player_name']
            players_diffs['player_2_name'] = player_2_details['player_name']
            players_diffs['player_1_url'] = player_1_details['player_url']
            players_diffs['player_2_url'] = player_2_details['player_url']

            for key, value in player_1_details.items():

                if type(value) in (float, int):

                    players_diffs[key] = round( player_1_details[key] - player_2_details[key] , 2 )

            upcoming_matches.append(players_diffs)
            
        upcoming_event_urls_processed.append(upcoming_event_url)
        
    # Add weight classes to upcoming_matches

    for i in range(len(upcoming_matches)):
        
        weight_class = players[upcoming_matches[i]['player_1_url']]['weight_class']

        upcoming_matches[i]['weight_class'] = weight_class


def handler(signal_received, frame):
    # SIGINT or  ctrl-C detected, exit without error
    exit(0)

# pylint: disable=C0103
app = Flask(__name__)
scheduler = APScheduler()


@app.route('/')
def hello():
    """Return a simple HTML page with a friendly message."""
    message = "It's running!"

    return render_template('index.html', message=message)


@app.route('/runLogs')
def runLogs():
    """Return a simple HTML page with a friendly message."""
    message = "test..."

    return render_template('index.html', message=message)

    
@app.route('/create-test-file')
def test_create_file():
    import csv
    import os
    from os.path import exists

    test_data=[['row1','aa1','bb1'], ['row2','aa2','bb2']]

   
    if exists('./test.csv'):
        print('removing test.csv')
        os.remove('./test.csv')
    
    with open('./test.csv', 'w+', newline='') as file:
        write = csv.writer(file)
        write.writerows(test_data)
        

    print('uploading test.csv to blob')
    upload_blob('ufc-scraper-data', './test.csv', 'test.csv')

    return render_template('index.html', message="upload test.csv suceeded")


@app.route('/show-test-file')
def show_test_file():

    import traceback
    from os.path import exists

    if exists('./downloaded_test.csv'):
        print('removing downloaded_test.csv')
        os.remove('./downloaded_test.csv')

    removeLocal_and_download('ufc-scraper-data', 'test.csv', './downloaded_test.csv')

    msg=''
    try:
        downloaded_data = pd.read_csv('./downloaded_test.csv', header=None)
        msg = str(downloaded_data.values.tolist())
    except:
        msg=traceback.format_exc()

    return render_template('index.html', message=msg)

@app.route('/scrape-data')
def scrape_data():
    print_to_stderr("Scraping starts...")
    scrape_completed_matches()
    print_to_stderr("Scraping completed...")

def print_to_stderr(*a):
    import sys
    # Here a is the array holding the objects
    # passed as the argument of the function
    print(*a, file=sys.stderr)
 
 
def job_function():
    # Do your work here
    print("Schedule...")
    print_to_stderr("StdErr Hello World")



if __name__ == '__main__':
    signal(SIGINT, handler)
    server_port = os.environ.get('PORT', '8080')
    print_to_stderr("Main... start...")
    scheduler.add_job(id = 'scheduled task', func = job_function, trigger = 'interval', seconds=3600)
    scheduler.start()
    # Shutdown your cron thread if the web process is stopped
    atexit.register(lambda: scheduler.shutdown(wait=False))

    app.run(debug=False, port=server_port, host='0.0.0.0')
 
    scrape_completed_matches()
    #scrape_upcoming_matches()
