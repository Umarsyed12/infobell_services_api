import tarfile

# open file
file = tarfile.open('wrapper.gz')

#checking
for i in file.getmembers():
    print(i)

# extracting file
#file.extractall('D:\infobell\pycharm\poc_pro\\venv')

file.close()

