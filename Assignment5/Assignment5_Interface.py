#
# Assignment5 Interface
# Name: Pranav Bajoria
# ASU ID: 1215321107
#

import os
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    if os.path.exists(saveLocation1):
        os.remove(saveLocation1)
    f = open(saveLocation1, 'a')
    # append the '^' and '$' symbols to make an exact pattern search
    exact_string = '^'+cityToSearch+'$'
    for doc in collection.find({"city": {"$regex": exact_string, "$options": "$i"}}): #use $i to make the search case insensitive
        f.write(('$'.join([doc["name"], doc["full_address"].replace("\n",", "), doc["city"], doc["state"]])).upper())
        f.write('\n')
    f.close()

#helper function to the FindBussineesBasedOnLocation to see whether the location is in range
def inRange(this_loc, my_loc, maxDist):
    R = 3959  # miles
    lat1 = math.radians(my_loc[0])
    lat2 = math.radians(this_loc[0])
    del_lat = math.radians((this_loc[0] - my_loc[0]))
    del_long = math.radians(this_loc[1] - my_loc[1])
    a = (math.sin(del_lat / 2) ** 2) + (math.cos(lat1) * math.cos(lat2) * (math.sin(del_long / 2) ** 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    # distance between the 2 locations in miles
    d = R * c
    if d <= maxDist:
        return True
    else:
        return False


def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    myLocation = (float(myLocation[0]), float(myLocation[1]))
    if os.path.exists(saveLocation2):
        os.remove(saveLocation2)
    f = open(saveLocation2, 'a')

    for doc in collection.find({"categories": {"$in": categoriesToSearch}}): #$in is used to choose any one of the listed categories
        thisLocation = (float(doc["latitude"]), float(doc["longitude"]))
        if inRange(thisLocation,myLocation,maxDistance):
            f.write(doc["name"].upper())
            f.write('\n')
    f.close()
