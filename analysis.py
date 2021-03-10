# -*- coding: utf-8 -*-
"""
Messenger Analysis
"""

import json
import os
import pandas as pd

rootdir = "C:\\Users\\jesse\\Documents\\Personal\\Messenger Analysis\messages\inbox"

all_messages_df = pd.DataFrame()

# Loop through messages/inbox folders
for subdir, dirs, files in os.walk(rootdir):    
    for i in range(1, 10):
        message_path = os.path.join(rootdir, subdir, "message_" + str(i) + ".json")
        if os.path.exists(message_path):
            # Read File
            with open(message_path) as message_data:
                message_json_data = json.load(message_data)
                
                # Only continue if it's a 1 on 1 chat
                if len(message_json_data['participants']) == 2:
                    print(message_path)
                    
                    person_name = [name for name in message_json_data['participants'] if name != "Jesse Cui"][0]
                    
                    message_df = pd.DataFrame(message_json_data['messages'])
                    message_df['friend'] = [person_name['name']] * len(message_df.index)
                    all_messages_df = all_messages_df.append(message_df)        

        else:
            break

# Sort ascending and by name
all_messages_df = all_messages_df.sort_values(["friend", "timestamp_ms"])

# Loop through messages per person
friends = all_messages_df['friend'].unique()

outreach_data = []

for friend in friends:
    print(friend)
    outreach_dict = {}

    # Subset data
    friend_df = all_messages_df.loc[all_messages_df['friend'] == friend].reset_index(drop=True)
    
    # Note down the person who started the entire convo
    outreach_dict['friend'] = friend
    outreach_dict['started'] = "yes" if (friend_df.iloc[0].sender_name == friend) else "no"
        
    # Loop through thread and note the thread starters and thread enders count (1 day between threads)
    jesse_init = 0
    friend_init = 0
    
    jesse_stop = 0
    friend_stop = 0
    
    prev_timestamp = 0
    prev_sender_is_friend = True    
    
    for index, row in friend_df.iterrows():
        if index == 0:        
            if row.sender_name == friend:
                friend_init += 1
            else:
                jesse_init += 1 
        elif index == (len(friend_df.index) - 1):
            if row.sender_name == friend:
                friend_stop += 1
            else:
                jesse_stop += 1 
        else:
            time_between = row.timestamp_ms- prev_timestamp
            
            if time_between > (86400000 * 3):
                if prev_sender_is_friend:
                    friend_stop += 1
                else:
                    jesse_stop += 1
                    
                if row.sender_name == friend:
                    friend_init += 1
                else:
                    jesse_init += 1                                        
            
        # update fields for next iteration
        prev_timestamp = row.timestamp_ms
        if row.sender_name == friend:
            prev_sender_is_friend = True
        else:
            prev_sender_is_friend = False
    
    outreach_dict['jesse_init'] = jesse_init
    outreach_dict['friend_init'] = friend_init
    outreach_dict['jesse_stop'] = jesse_stop
    outreach_dict['friend_stop'] = friend_stop
    
    outreach_data.append(outreach_dict)
    
outreach_df = pd.DataFrame(outreach_data)

outreach_df['jesse_init_more'] = outreach_df['jesse_init'] - outreach_df['friend_init']
outreach_df['jesse_stop_more'] = outreach_df['jesse_stop'] - outreach_df['friend_stop']

outreach_df['score'] = outreach_df['jesse_init_more'] + outreach_df['jesse_stop_more']

outreach_df = outreach_df.sort_values("score")

outreach_df  = outreach_df.to_csv("final_results.csv")


# Post analysis
more = sum(outreach_df['jesse_init_more'] > 1)
less = sum(outreach_df['jesse_init_more'] < -1)
even = len(outreach_df.index) - more - less

init_ratio = less / more

more = sum(outreach_df['jesse_stop_more'] > 1)
less = sum(outreach_df['jesse_stop_more'] < -1)
even = len(outreach_df.index) - more - less

stop_ratio = less / more
