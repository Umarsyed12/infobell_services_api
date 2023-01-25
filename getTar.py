import subprocess
import os
import fnmatch
# Get the ID of the latest running container
container_id = subprocess.run(["docker", "ps", "-qa"], capture_output=True, text=True).stdout.strip().split("\n")[0]

print("Latest container ID:", container_id)
#pattern = 'wrapper_.tar.gz'
file_name = input("Enter the file name : ")
os.system('docker cp '+container_id+':/'+file_name+' .')


