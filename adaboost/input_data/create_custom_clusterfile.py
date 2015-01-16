import sys
import glob
import os

#data_dir = str(sys.argv[1])
file_filter = str(sys.argv[1])
cluster_file_name = str(sys.argv[2])

partition_files = glob.glob(file_filter+"*")

cluster_file = open(cluster_file_name,'w')

ip_address = "129.74.246.88"

for i in range(len(partition_files)):
	cluster_file.write(str(i) + "," + ip_address + "," + "5," + os.path.abspath(partition_files[i])+"\n")
	
cluster_file.close()
