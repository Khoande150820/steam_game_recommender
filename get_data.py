from urllib.request import urlopen
import requests, json, os, sys, time, re
import pandas as pd
import numpy as np
from utils import show_work_status


# Get game data from steam
def get_game_data():
    game_data = requests.get('https://steamspy.com/api.php?request=top100forever')
    game_data = game_data.json()
    return game_data


# Get median game time

def get_game_time(game_data):
    total_count = len(game_data)
    dic_game_time = {'appid': {}, 'name': {}, 'median_forever': {}}
    current_count = 0
    for game_id, game_details in game_data.items():
        if game_details != {} and game_details is not None:
            dic_game_time['appid'].update({game_id: game_details.get("appid", {})})
            dic_game_time['name'].update({game_id: game_details.get('name', {})})
            dic_game_time['median_forever'].update({game_id: game_details.get('median_forever', {})})
        show_work_status(1, total_count, current_count)
        current_count += 1
    return dic_game_time


# Get user link
def get_user_links():
    df_user_links = pd.DataFrame(columns=['Links'])

    with open('users.txt', 'r') as f:
        for line in f:
            link = line.strip()
            df_user_links.loc[len(df_user_links.index)] = link
    return df_user_links


def get_game_play_median(dic_game_time):
    game_play_median = pd.DataFrame(dic_game_time)
    game_play_median = game_play_median.rename(columns={'median_forever': 'median_playtime(min)'})
    return game_play_median


# Get playtime data
def get_df(aa, game_play_median):
    ss = aa.json()
    sd = pd.DataFrame(ss['response']['games'])
    no_zero = sd.loc[sd['playtime_forever'] != 0]
    df_col = pd.merge(no_zero, game_play_median, on="appid")
    return df_col


# Get rating
def get_ratings(df_col):
    df_col['Assuming_Ratings'] = 0
    for i in range(len(df_col)):
        # If user playing time greater than median playing time of all player then rating is 5
        if df_col['playtime_forever'][i] >= df_col['median_playtime(min)'][i]:
            df_col['Assuming_Ratings'][i] = 5
        # If user playing time is greater than 0.8 median playing time then rating is 4
        elif df_col['median_playtime(min)'][i] > df_col['playtime_forever'][i] >= df_col['median_playtime(min)'][
            i] * 0.8:
            df_col['Assuming_Ratings'][i] = 4
        # If user playing time is greater than 0.5 median playing time then rating is 3
        elif df_col['median_playtime(min)'][i] * 0.8 > df_col['playtime_forever'][i] >= df_col['median_playtime(min)'][
            i] * 0.5:
            df_col['Assuming_Ratings'][i] = 3
        # If user playing time is greater than 0.5 median playing time then rating is 2
        elif df_col['median_playtime(min)'][i] * 0.5 > df_col['playtime_forever'][i] >= df_col['median_playtime(min)'][
            i] * 0.1:
            df_col['Assuming_Ratings'][i] = 2
        # If user playing time is greater than 0.5 median playing time then rating is 1
        elif df_col['median_playtime(min)'][i] * 0.1 > df_col['playtime_forever'][i]:
            df_col['Assuming_Ratings'][i] = 1
    return df_col


def RDD_csv(df_col, i):
    df_col_text = df_col
    df_col_text['user_id'] = i
    return df_col_text


def get_data(df_user_links, game_play_median):
    df_col_text_ = []
    df_col_a1 = get_ratings(get_df(requests.get(df_user_links['Links'][0]), game_play_median))
    df_col_text_ = RDD_csv(df_col_a1, 0)
    df_col_text_ = df_col_text_.drop(df_col_text_.index[0:len(df_col_text_)], axis=0)
    j = 0
    while j < 40:
        for i in range(8):
            df_col_ = get_ratings(
                get_df(requests.get(df_user_links['Links'][i]), game_play_median))  ###### Anti_crawler when i >= 9

            df_col_text_element = RDD_csv(df_col_, i)
            df_col_text_ = df_col_text_.append(df_col_text_element)
            j += 1
    df_col_text__csv = df_col_text_[
        ['user_id', 'appid', 'Assuming_Ratings']]  # user_id,appid,Game_name,Assuming_Ratings
    df_col_text__csv.to_csv('user_game_rating.csv')


game_data = get_game_data()
df_user_link = get_user_links()

dic_game_time = get_game_time(game_data)
game_play_median = get_game_play_median(dic_game_time)

get_data(df_user_link, game_play_median)