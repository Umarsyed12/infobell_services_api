import tarfile

# Open the tar file
tar = tarfile.open("wrapper_idrac-2tmmk93-os_20221202_091059.tar.gz")

# Extract all the contents of the tar file to the current working directory
tar.extractall()

# Close the tar file
#tar.close()

#Get a list of all the files in the tar file
files = tar.getnames()
print(files)

""""""

# Iterate over the list of files
for file in files:
    # Open the file
    f = open(file, "r")

    # Read the contents of the file
    contents = f.read()

    # Print the contents of the file
    print(contents)

    # Close the file
    f.close() '''


import json
import os, json
import pandas as pd

path_to_json = 'E:/flask/wrapper_idrac-2tmmk93-os_20221202_091059'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
print(json_files)
for i in json_files:
    print(i)

    f = open(i, "r")
    contents = f.read()

    print(contents)

    f.close()
'''    f = open('i')

    # returns JSON object as
    # a dictionary
    data = json.load(f)

    # Iterating through the json
    # list
    for a in data:
        print(a)'''

