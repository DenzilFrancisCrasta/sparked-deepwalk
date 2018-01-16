import matplotlib.pyplot as plt
import csv 
import math 
import sys


def readCSV(datafile, schema):
  data = [[] for header in schema]
  with open(datafile, 'rb') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
      for i in range(len(schema)):
        data[i].append(row[schema[i]])

  return data

def plot(title, xlabel, ylabel, x, y, marker, xticks=[], yticks=[]):
  plt.plot(x, y, marker)
  if xticks:
    plt.xticks(xTicks)
  if yticks:
    plt.yticks(yTicks)
  plt.title(title)
  plt.xlabel(xlabel)
  plt.ylabel(ylabel)
  plt.show()

  

datafile = sys.argv[1]
dataset = sys.argv[2]

data = readCSV(datafile, ['numberOfVisits', 'numberOfVertices']) 
x = [int(a) for a in data[0][:]]
y = [int(a) for a in data[1][:]]
maxVisits = max(x)
maxVertices = max(y)
xTicks = [i for i in range(15) if pow(10, i) < maxVisits] 
yTicks = [i for i in range(15) if pow(10, i) < maxVertices] 
xTicks.insert(0, -0.1)
yTicks.insert(0, -0.1)

logx = [math.log(v, 10) for v in x]
logy = [math.log(v, 10) for v in y]

plot(
  dataset + " - Frequency of Vertex Occurrence in Short Random Walks",
  "Vertex Visitation Count",
  "# of Vertices",
  logx, 
  logy,
  'b+',
  xTicks,
  yTicks)


data = readCSV("output/"+ dataset +"_vec.csv", ["dim1", "dim2"])
x = [float(a) for a in data[0][:]]
y = [float(a) for a in data[1][:]]
plot(
  dataset + " vectors ",
  "dimension 1",
  "dimension 2",
  x,
  y,
  'bo')






