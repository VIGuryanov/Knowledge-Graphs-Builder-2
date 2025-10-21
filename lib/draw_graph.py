from lib.triple import Triple
import networkx as nx
import matplotlib.pyplot as plt
import copy
from lib.standard_predicates import StandardPredicates

class DrawGraph:

    def __init__(self):
        self.__graph = nx.DiGraph()

    def __replace_with_labels(self, data: list[Triple]):
        data = copy.deepcopy(data)

        labels = {}
        for triple in data:
            if triple.predicate.value == StandardPredicates._label.value or triple.predicate.value == 'http://xmlns.com/foaf/0.1/name':
                labels[triple.subject.value] = triple.object.value

        data = [a for a in data if a.predicate.value != StandardPredicates._label.value and a.predicate.value != 'http://xmlns.com/foaf/0.1/name']
        
        for triple in data:
            if triple.subject.value in labels:
                triple.subject.value = labels[triple.subject.value]
            
            if triple.object.value in labels:
                triple.object.value = labels[triple.object.value]

            if triple.predicate.value in labels:
                triple.predicate.value = labels[triple.predicate.value]
        
        return data

    def graph_add(self, data: list[Triple], labels_instead_iri = False):

        if labels_instead_iri:
            data = self.__replace_with_labels(data)

        for tr in data:
            self.__graph.add_edge(tr.subject.value, tr.object.value, val = tr.predicate.value)
    
    def draw_graph(self, size = 300, output_path = 'graph.png'):
        plt.figure(figsize=(size, size))
        pos = nx.fruchterman_reingold_layout(self.__graph)
        nx.draw_networkx(self.__graph, pos, with_labels=True)
        labels = nx.get_edge_attributes(self.__graph, 'val')
        nx.draw_networkx_edge_labels(self.__graph, pos, edge_labels=labels)
        axis = plt.gca()
        axis.set_xlim([1.5*x for x in axis.get_xlim()])
        axis.set_ylim([1.5*y for y in axis.get_ylim()])
        plt.tight_layout()
        plt.savefig(output_path)