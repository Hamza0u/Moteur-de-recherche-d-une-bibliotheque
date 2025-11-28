import os
import re
import json
from collections import defaultdict, deque
from django.conf import settings
from django.core.management.base import BaseCommand
from elasticsearch import Elasticsearch

class BookGraph:
    def __init__(self, books_dir):
        self.books_dir = books_dir
        self.graph = {}
        self.book_ids = []
        self.jaccard_similarities = {}
        self.es = Elasticsearch("http://localhost:9200", timeout=60)

    def build_jaccard_graph(self):
        """Construit le graphe de similarité Jaccard entre les livres"""
        print("Construction du graphe Jaccard...")
        book_word_sets = {}
        self.book_ids = []
        
        for filename in os.listdir(self.books_dir):
            if not filename.endswith(".txt"):
                continue
            match = re.match(r"(\d+)", filename)
            if not match:
                continue
            book_id = match.group(1)
            self.book_ids.append(book_id)
            filepath = os.path.join(self.books_dir, filename)
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read().lower()
            words = re.findall(r"[a-zàâçéèêëîïôûùüÿñœ]+", content)
            book_word_sets[book_id] = set(words)
            self.graph[book_id] = {}
        
        print(f"{len(self.book_ids)} livres chargés")
        self.jaccard_similarities = {}
        
        for i, book1 in enumerate(self.book_ids):
            if i % 100 == 0:
                print(f"Calcul des similarités... {i}/{len(self.book_ids)}")
            set1 = book_word_sets[book1]
            for book2 in self.book_ids[i+1:]:
                set2 = book_word_sets[book2]
                intersection = len(set1.intersection(set2))
                union = len(set1.union(set2))
                if union > 0:
                    similarity = intersection / union
                    if similarity > 0.01:  # Seuil de similarité
                        self.graph[book1][book2] = similarity
                        self.graph[book2][book1] = similarity
                        self.jaccard_similarities[(book1, book2)] = similarity
                        self.jaccard_similarities[(book2, book1)] = similarity
        
        print(f"Graphe construit avec {len(self.graph)} nœuds et {sum(len(neighbors) for neighbors in self.graph.values()) // 2} arêtes")

    def save_graph_to_es(self):
        """Sauvegarde le graphe Jaccard dans Elasticsearch"""
        print("\n💾 Sauvegarde du graphe Jaccard dans Elasticsearch...")
        
        # Créer l'index pour le graphe s'il n'existe pas
        if not self.es.indices.exists(index="jaccard_graph"):
            self.es.indices.create(index="jaccard_graph", body={
                "mappings": {
                    "properties": {
                        "book_id": {"type": "keyword"},
                        "neighbors": {"type": "flattened"}  # Pour stocker les voisins et leurs similarités
                    }
                }
            })
            print("Index 'jaccard_graph' créé")
        
        # Sauvegarder chaque livre avec ses voisins
        from elasticsearch.helpers import bulk
        actions = []
        for book_id, neighbors in self.graph.items():
            actions.append({
                "_index": "jaccard_graph",
                "_id": book_id,
                "_source": {
                    "book_id": book_id,
                    "neighbors": neighbors
                }
            })
        
        success, failed = bulk(self.es, actions, raise_on_error=False)
        print(f"Graphe sauvegardé: {success} livres, {len(failed)} échecs")
        return success

    def load_graph_from_es(self):
        """Charge le graphe Jaccard depuis Elasticsearch"""
        print("Chargement du graphe Jaccard depuis Elasticsearch...")
        
        if not self.es.indices.exists(index="jaccard_graph"):
            print("Index 'jaccard_graph' non trouvé")
            return False
        
        # Charger tous les documents
        resp = self.es.search(
            index="jaccard_graph",
            body={"query": {"match_all": {}}},
            size=10000
        )
        
        self.graph = {}
        for hit in resp["hits"]["hits"]:
            source = hit["_source"]
            book_id = source["book_id"]
            neighbors = source.get("neighbors", {})
            self.graph[book_id] = neighbors
        
        print(f"Graphe chargé: {len(self.graph)} livres")
        return len(self.graph) > 0

    def compute_closeness(self):
        """Calcule la closeness centrality"""
        print("Calcul de la closeness centrality...")
        if not self.graph:
            self.build_jaccard_graph()
            
        closeness = {}
        for i, source in enumerate(self.book_ids):
            if i % 50 == 0:
                print(f"Closeness: {i}/{len(self.book_ids)}")
            
            dist = {book_id: float('inf') for book_id in self.book_ids}
            dist[source] = 0
            visited = set()
            
            while len(visited) < len(self.book_ids):
                current = None
                min_dist = float('inf')
                for book_id in self.book_ids:
                    if book_id not in visited and dist[book_id] < min_dist:
                        min_dist = dist[book_id]
                        current = book_id
                if current is None:
                    break
                    
                visited.add(current)
                for neighbor, weight in self.graph.get(current, {}).items():
                    if weight > 0:
                        distance = 1.0 / weight
                        new_dist = dist[current] + distance
                        if new_dist < dist[neighbor]:
                            dist[neighbor] = new_dist
            
            total_distance = sum(d for d in dist.values() if d != float('inf'))
            reachable_nodes = sum(1 for d in dist.values() if d != float('inf'))
            if total_distance > 0 and reachable_nodes > 1:
                closeness[source] = (reachable_nodes - 1) / total_distance
            else:
                closeness[source] = 0
                
        return closeness

    def save_scores_to_es(self, closeness):
        """Sauvegarde les scores dans Elasticsearch"""
        print("\nSauvegarde des scores dans Elasticsearch...")
        if not self.es.indices.exists(index="book_scores"):
            self.es.indices.create(index="book_scores", body={
                "mappings": {
                    "properties": {
                        "book_id": {"type": "keyword"},
                        "closeness": {"type": "float"}
                    }
                }
            })
            print("Index 'book_scores' créé")
            
        from elasticsearch.helpers import bulk
        actions = []
        for book_id in self.book_ids:
            actions.append({
                "_index": "book_scores",
                "_id": book_id,
                "_source": {
                    "book_id": book_id,
                    "closeness": closeness.get(book_id, 0.0)
                }
            })
        bulk(self.es, actions)
        print(f"Scores sauvegardés pour {len(actions)} livres")

    def load_scores_from_es(self):
        """Charge les scores depuis Elasticsearch"""
        if not self.es.indices.exists(index="book_scores"):
            return None
            
        resp = self.es.search(
            index="book_scores",
            body={"query": {"match_all": {}}},
            size=10000
        )
        closeness = {}
        for hit in resp["hits"]["hits"]:
            source = hit["_source"]
            book_id = source["book_id"]
            closeness[book_id] = source["closeness"]
        return closeness

    def get_graph_stats(self):
        """Retourne les statistiques du graphe"""
        nodes = len(self.graph)
        edges = sum(len(neighbors) for neighbors in self.graph.values()) // 2
        return {"nodes": nodes, "edges": edges}

# Instance globale
BOOKS_DIR = os.path.join(settings.BASE_DIR, "moteur", "books", "gutendex_books")
book_graph = BookGraph(BOOKS_DIR)

def initialize_graph():
    """Initialise le graphe et sauvegarde les scores dans ES"""
    try:
        book_graph.build_jaccard_graph()
        print("\nCalcul de la closeness centrality...")
        closeness = book_graph.compute_closeness()
        book_graph.save_scores_to_es(closeness)
        book_graph.save_graph_to_es()  # NOUVEAU: Sauvegarde le graphe
        print("\nGraphe Jaccard initialisé et sauvegardé dans ES!")
        return True
    except Exception as e:
        print(f"Erreur lors de l'initialisation du graphe: {e}")
        return False

class Command(BaseCommand):
    help = 'Initialise le graphe de similarité Jaccard'

    def handle(self, *args, **options):
        self.stdout.write('Initialisation du graphe Jaccard...')
        success = initialize_graph()
        if success:
            self.stdout.write(
                self.style.SUCCESS(' Graphe initialisé avec succès!')
            )
        else:
            self.stdout.write(
                self.style.ERROR(' Erreur lors de l\'initialisation du graphe')
            )