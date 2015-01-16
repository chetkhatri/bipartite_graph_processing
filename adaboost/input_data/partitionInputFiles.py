import sys

inputFile = str(sys.argv[1])
fileFilter = str(sys.argv[2])
numPartitions = int(str(sys.argv[3]))

partitionFileStrings=[]

for i in range(0, numPartitions):
	partitionFileStr = fileFilter + "_" + str(i) + ".txt"
	#print "partition file %i: %s" % (i, partitionFileStr)
	partitionFileStrings.append(partitionFileStr)

with open(inputFile, 'r') as f:
	firstline = f.readline()
	numInstances = int(firstline.split('\t')[0])
	#print str(numInstances)
	remainder = numInstances % numPartitions
	reg_part_size = numInstances / numPartitions
	mod_part_size = reg_part_size + 1
	
	line_counter=0
	file_counter=0
	
	partitionFile=""
	
	for line in f: 
		if line_counter == 0:
			#print file_counter
			#print len(partitionFileStrings)
			partitionFile = open(partitionFileStrings[file_counter],'w')
			if file_counter < (numPartitions-remainder):
				partitionFile.write(str(reg_part_size)+"\n")
			else:
				partitionFile.write(str(reg_part_size+1)+"\n")
		partitionFile.write(line)
		line_counter+=1
		if line_counter == reg_part_size and file_counter < (numPartitions-remainder):
			partitionFile.close()
			file_counter+=1
			line_counter=0
		if line_counter == mod_part_size:
			partitionFile.close()
			file_counter+=1
			line_counter=0