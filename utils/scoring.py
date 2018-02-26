#!/usr/bin/env python

"""scoring.py: Script that demonstrates the multi-label classification used."""

__author__      = "Denzil Crasta"

import numpy
import sys

import csv

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from collections import defaultdict
#from gensim.models import Word2Vec, KeyedVectors
from six import iteritems
from sklearn.multiclass import OneVsRestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score
from scipy.io import loadmat
from sklearn.utils import shuffle as skshuffle
from sklearn.preprocessing import MultiLabelBinarizer
from collections import defaultdict

class TopKRanker(OneVsRestClassifier):
    def predict(self, X, top_k_list):
        assert X.shape[0] == len(top_k_list)
        probs = numpy.asarray(super(TopKRanker, self).predict_proba(X))
        all_labels = []
        for i, k in enumerate(top_k_list):
            probs_ = probs[i, :]
            labels = self.classes_[probs_.argsort()[-k:]].tolist()
            all_labels.append(labels)
        return all_labels


def main():
  parser = ArgumentParser("scoring",
                          formatter_class=ArgumentDefaultsHelpFormatter,
                          conflict_handler='resolve')
  parser.add_argument("--seed", default=500, type=int, help='seed value for random operations')
  parser.add_argument("--emb", required=True, help='Embeddings file')
  parser.add_argument("--edgelist", required=True,
                      help='A csv file containing the edges of the input network.')
  parser.add_argument("--labellist", required=True,
                      help='A csv file containing the labels of the nodes from the  input network.')
  parser.add_argument("--num-shuffles", default=1, type=int, help='Number of shuffles.')
  parser.add_argument("--all", default=False, action='store_true',
                      help='The embeddings are evaluated on all training percents from 10 to 90 when this flag is set to true. '
                      'By default, only training percents of 10, 50 and 90 are used.')

  args = parser.parse_args()
  seed = args.seed
  # 0. Files
  embeddings_file = args.emb
  edgelist_file = args.edgelist
  labellist_file = args.labellist
  
  # 1. Load Embeddings
  #model = KeyedVectors.load_word2vec_format(embeddings_file, binary=False)

  model = {}
  with open(embeddings_file) as emb_file:
    reader = csv.reader(emb_file, delimiter=' ')
    header = reader.next()
    print(header[0], header[1])
    for row in reader:
      model[row[0]] = [float(k) for k in row[1:]]

  
  graph_nodes = set()

  with open(edgelist_file) as edge_file:
    reader = csv.DictReader(edge_file, ['vertex_1', 'vertex_2'])
    for row in reader:
      graph_nodes.add(row['vertex_1'])
      graph_nodes.add(row['vertex_2'])


  labels_map = defaultdict(list)

  with open(labellist_file) as label_file:
    reader = csv.DictReader(label_file, ['vertex', 'label'])
    for row in reader:
      labels_map[row['vertex']].append(int(row['label']))

  ordered_labels = [labels_map[str(node)] for node in range(1,len(graph_nodes)+1) if str(node) in  model ]

  all_labels = set()
  for l in labels_map.itervalues():
    all_labels |= set(l)

  labels_count = len(all_labels)
  mlb = MultiLabelBinarizer(range(labels_count))
 
  llb = MultiLabelBinarizer(range(1, labels_count+1), sparse_output=True)
  labels_matrix = llb.fit_transform(ordered_labels)

  
  # Map nodes to their features (note:  assumes nodes are labeled as integers 1:N)
  features_matrix = numpy.asarray([model[str(node)] for node in range(1,len(graph_nodes)+1) if str(node) in model ])
  
  # 2. Shuffle, to create train/test groups
  shuffles = []
  for x in range(args.num_shuffles):
    shuffles.append(skshuffle(features_matrix, labels_matrix, random_state=seed))
  
  # 3. to score each train/test group
  all_results = defaultdict(list)
  
  if args.all:
    training_percents = numpy.asarray(range(1, 10)) * .1
  else:
    training_percents = [0.1, 0.5, 0.9]
  for train_percent in training_percents:
    for shuf in shuffles:
  
      X, y = shuf
  
      training_size = int(train_percent * X.shape[0])
  
      X_train = X[:training_size, :]
      y_train_ = y[:training_size]
  
      y_train = [[] for x in range(y_train_.shape[0])]
  
  
      cy =  y_train_.tocoo()
      for i, j in zip(cy.row, cy.col):
          y_train[i].append(j)
  
      assert sum(len(l) for l in y_train) == y_train_.nnz
  
      X_test = X[training_size:, :]
      y_test_ = y[training_size:]
  
      y_test = [[] for _ in range(y_test_.shape[0])]
  
      cy =  y_test_.tocoo()
      for i, j in zip(cy.row, cy.col):
          y_test[i].append(j)
  
      clf = TopKRanker(LogisticRegression(random_state=seed))
      clf.fit(X_train, y_train_)
  
      # find out how many labels should be predicted
      top_k_list = [len(l) for l in y_test]
      preds = clf.predict(X_test, top_k_list)
  
      results = {}
      averages = ["micro", "macro"]
      for average in averages:
          results[average] = f1_score(mlb.fit_transform(y_test), mlb.fit_transform(preds), average=average)
  
      all_results[train_percent].append(results)
  
  print ('Results, using embeddings of dimensionality', X.shape[1])
  print ('-------------------')
  for train_percent in sorted(all_results.keys()):
    print ('Train percent:', train_percent)
    for index, result in enumerate(all_results[train_percent]):
      print ('Shuffle #%d:   ' % (index + 1), result)
    avg_score = defaultdict(float)
    for score_dict in all_results[train_percent]:
      for metric, score in iteritems(score_dict):
        avg_score[metric] += score
    for metric in avg_score:
      avg_score[metric] /= len(all_results[train_percent])
    print ('Average score:', dict(avg_score))
    print ('-------------------')

if __name__ == "__main__":
  sys.exit(main())
