import networkx as nx
from elasticsearch import Elasticsearch
import json

es = Elasticsearch("http://localhost:9200")

def load_inverted_index():
    resp = es.search(index="inverse_index", body={"query": {"match_all": {}}}, size=200000)
    index = {}
    for hit in resp["hits"]["hits"]:
        term = hit["_source"]["term"]
        index[term] = hit["_source"]["books"]
    return index

def compute_jaccard_graph(index, threshold=0.1):
    # Construire le vocabulaire par livre
    book_terms = {}
    for term, books in index.items():
        for book_id in books.keys():
            book_terms.setdefault(book_id, set()).add(term)

    books = list(book_terms.keys())
    G = nx.Graph()

    # Ajouter les noeuds
    for b in books:
        G.add_node(b)

    # Calcul Jaccard pairwise
    for i in range(len(books)):
        for j in range(i+1, len(books)):
            A = book_terms[books[i]]
            B = book_terms[books[j]]

            inter = len(A & B)
            union = len(A | B)
            if union == 0:
                continue

            jaccard = inter / union

            if jaccard >= threshold:
                G.add_edge(books[i], books[j], weight=jaccard)

    return G


def compute_pagerank():
    index = load_inverted_index()
    G = compute_jaccard_graph(index)

    pr = nx.pagerank(G, weight="weight")

       # Sauvegarde
    with open("pagerank_scores.json", "w") as f:
        json.dump(pr, f)

    return pr
