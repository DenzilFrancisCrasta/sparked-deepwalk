import matplotlib.pyplot as plt
import csv 
import math 
import sys


datafile = sys.argv[1]
dataset = sys.argv[2]

x = []
y = []
with open(datafile, 'rb') as csvfile:
  reader = csv.DictReader(csvfile)
  for row in reader:
    x.append(int(row['numberOfVisits']))
    y.append(int(row['numberOfVertices']))

maxVisits = max(x)
maxVertices = max(y)


xTicks = [i for i in range(15) if pow(10, i) < maxVisits] 
yTicks = [i for i in range(15) if pow(10, i) < maxVertices] 
xTicks.insert(0, -0.1)
yTicks.insert(0, -0.1)

logx = [math.log(v, 10) for v in x]
logy = [math.log(v, 10) for v in y]


plt.plot(logx, logy, 'b+')
plt.xticks(xTicks)
plt.yticks(yTicks)
plt.title(dataset + " - Frequency of Vertex Occurrence in Short Random Walks")
plt.xlabel("Vertex Visitation Count")
plt.ylabel("# of Vertices")
plt.show()
