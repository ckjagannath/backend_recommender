from flask import Flask,request
from flask_cors import CORS
import re
import pandas as pd
from pandas import DataFrame as df
import json
from topmov_recommender import merged_moviedata, build_chart

app = Flask(__name__)
cors = CORS(app)


"""
    This reads movieinfo containing movieid from 1 to 1682 and their corresponding titles
"""
movies_info = pd.read_csv('ml-100k/u.item', sep='|', header=None, encoding='ISO-8859-1', usecols=[0, 1])
movies_info.columns = ['movieId', 'title']
# print(movies_info)


"""
    To get the tmdbids we match the title column in both and get them 
"""
movies_info = df1 = movies_info.assign(exist=movies_info['title'].isin(merged_moviedata['title']))
tmdbids = pd.read_csv('ml-100k/100K_idsOfUnmatchedtitles.csv', skip_blank_lines=False) #contains tmdbids of unmatched titles(i.e. with minor changes can be even 1 letter)
movies_info_false = df1[df1['exist'] == False]
movies_info_false.insert(3, 'tmdbId', tmdbids.values)
# print(movies_info_false)
movies_info_false.drop('exist', axis=1)
movies_info_true = df1[df1['exist'] == True]
movies_info = df.merge(merged_moviedata, movies_info_true, on='title', how='inner', left_index=True)
movies_info = movies_info[['movieId_y', 'title', 'tmdbId']]
movies_info.columns = ['movieId', 'title', 'tmdbId']
movies_info = movies_info.append(movies_info_false).sort_values('movieId').drop('exist', axis=1).drop_duplicates('movieId')
# print(movies_info)


movieNames = movies_info['title']
"""
    taking the title column and preparing a new dataframe of lower case titles from it
    without changing the index, Line: 39 & 40
"""
movieNamesInLowerCase = list(map(lambda y:y.lower(),movieNames))  
df2 = df(movieNamesInLowerCase, columns=['lowercasetitles'])

@app.route('/api/search', methods=['POST'])
def searchString():
    data = json.loads(request.data.decode('utf-8'))
    searchstring = data['searchstring']
    if len(searchstring) == 0:              # if search is empty return []
        return json.dumps([])
    x = re.compile(searchstring.lower())    #converting searchstring to regex object
       
    matched = list(filter(x.search,movieNamesInLowerCase))    #matched movienames from searchstring
    """
        filter returns an iterator of the 'true' based values from re.search() 
        and applying list() to it actauuly gives the matched values
    """
    # print(matched)
    
    matched = list(map(lambda z: df2[df2['lowercasetitles'] == z].index.tolist(), matched)) # getting indexes of matched titles
    matched = list(map(lambda z: (movieNames[z[0]], movies_info['tmdbId'][z[0]]), matched)) #using indexes retrieving the actual titles from movieNames frame so originally present case is maintained
    
    return json.dumps(matched[0:5]) # returning first five matches if present

@app.route('/api/genre', methods=['POST'])
def getChartbyGenre():
    data = json.loads(request.data.decode('utf-8'))
    genre = data["genre"]
    chart_df = build_chart(genre).head(15)
    ids = list(chart_df['tmdbId'].values)
    return json.dumps(ids)
    