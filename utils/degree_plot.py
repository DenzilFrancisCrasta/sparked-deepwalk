import matplotlib.pyplot as plt
import csv 
import sys


datafile = sys.argv[1]
dataset = sys.argv[2]

print(datafile)
print(dataset)

x = []
y = []
with open(datafile, 'rb') as csvfile:
  reader = csv.DictReader(csvfile)
  for row in reader:
    x.append(int(row['degree']))
    y.append(int(row['frequency']))


plt.plot(x, y, 'b+')
plt.title(dataset + " - Frequency of Vertex Occurrence in Short Random Walks")
plt.xlabel("Vertex Visitation Count")
plt.ylabel("# of Vertices")
plt.show()

