# importing the "tarfile" module
import tarfile

# open file
file = tarfile.open('wrapper.gz')

# extracting file
file.extractall('./temp')

file.close()
