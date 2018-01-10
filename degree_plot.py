import matplotlib.pyplot as plt
import csv 

x = []
y = []
with open("degree_dist.csv", 'rb') as csvfile:
  reader = csv.DictReader(csvfile)
  for row in reader:
    x.append(int(row['degree']))
    y.append(int(row['frequency']))


plt.plot(x, y, 'b+')
plt.title("Blog Catalog - Frequency of Vertex Occurrence in Short Random Walks")
plt.xlabel("Vertex Visitation Count")
plt.ylabel("# of Vertices")
plt.show()

