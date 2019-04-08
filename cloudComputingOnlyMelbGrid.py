from mpi4py import MPI
import pandas as pd
import numpy as np
from collections import Counter
import time
import json

count_tweet = {}
melb_grid = {}
hash_dict = [None]*16
with open('melbGrid.json', 'r',encoding="utf-8") as f:
    data = json.load(f)

for index,feature in enumerate(data['features']):
    count_tweet[feature['properties']['id']] = 0
    hash_dict[index] = {}

    melb_grid[feature['properties']['id']] = [feature['properties']['xmin'],
                                               feature['properties']['xmax'],
                                               feature['properties']['ymin'],
                                               feature['properties']['ymax']]



comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
name =  comm.Get_name()


if rank==0:

    tweetlist = []
    with open('file_data.json','r',encoding="utf-8") as fp:
        #check = json.load(fp)
        #print(check['total_rows'])
        head = [next(fp) for x in range(2)]
        for each in fp:
            tweet_dict = {}
            each = each.rstrip("]" + "[" + "," + "\n")
            if len(each) > 3:
                try:
                    row = json.loads(each)
                    tweet_dict['x'] = row["doc"]['coordinates']['coordinates'][0]
                    tweet_dict['y'] = row["doc"]['coordinates']['coordinates'][1]
                    if tweet_dict['x'] < melb_grid['A1'][0] or tweet_dict['x'] > melb_grid['D5'][1] or tweet_dict['y'] > melb_grid['A1'][2] or tweet_dict['y'] < melb_grid['D5'][3]:
                        continue
                    hashtags = row['doc']['entities']['hashtags']
                    hashlist = []
                    for hashtg in hashtags:
                        hashlist.append(hashtg['text'])
                    tweet_dict['hashlist'] = hashlist
                    tweetlist.append(tweet_dict)
                except:
                     continue

    fp.close()
    tweet_pieces = np.array_split(tweetlist,size)
else:
    tweet_pieces = None

tweet_pieces = comm.scatter(tweet_pieces, root=0)

for tweet in tweet_pieces:
    x = tweet['x']
    y = tweet['y']
    hashlist = tweet['hashlist']
    
    for index,block in enumerate(melb_grid):
        xmin = melb_grid[block][0]
        xmax = melb_grid[block][1]
        ymin = melb_grid[block][2]
        ymax = melb_grid[block][3]        
        
        if xmin<= x and x < xmax and y >= ymin and y < ymax:
            count_tweet[block] += 1

            for hashtag in hashlist:
                if hashtag not in hash_dict[index-1]:
                    hash_dict[index-1][hashtag] = 1
                else:
                    hash_dict[index-1].update({hashtag:hash_dict[index-1][hashtag]+1})

comm.Barrier()
tweet_gather = comm.gather(count_tweet,root=0)
hashtag_gather = comm.gather(hash_dict,root=0)

if rank == 0:

    hashtag_list = [None]*len(melb_grid)
    count_tweet_gather = Counter()
    for block in tweet_gather:
        count_tweet_gather.update(block)

    print(count_tweet_gather)

    for key_idx,keys in enumerate(melb_grid.keys()):
        hashtag_list[key_idx] = Counter()
    for chunk in hashtag_gather:
        for idx,block in enumerate(chunk):
            hashtag_list[idx].update(block)

    for idx,key in enumerate(melb_grid.keys()):
        print(key, hashtag_list[idx-1].most_common(5))
