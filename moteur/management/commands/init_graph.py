# management/commands/init_graph.py
import os
import re
from collections import defaultdict, deque
from django.conf import settings
from django.core.management.base import BaseCommand

class BookGraph:
    def __init__(self, books_dir):
        self.books_dir = books_dir
        self.graph = {}  # {book_id: {neighbor: similarity}}
        self.book_ids = []
        self.jaccard_similarities = {}
        
    def build_jaccard_graph(self):
        """Construit le graphe de similarité Jaccard entre les livres"""
        print("Construction du graphe Jaccard...")
        
        # Étape 1: Extraire les ensembles de mots pour chaque livre
        book_word_sets = {}
        self.book_ids = []
        
        for filename in os.listdir(self.books_dir):
            if not filename.endswith(".txt"):
                continue
                
            # ⬅️ MÊME EXTRACTION que dans build_inverted_index
            match = re.match(r"(\d+)", filename)
            if not match:
                continue
                
            book_id = match.group(1)  # ⬅️ STRING, pas int
            self.book_ids.append(book_id)
            
            filepath = os.path.join(self.books_dir, filename)
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read().lower()
            
            # ⬅️ MÊME TOKENIZATION que dans build_inverted_index
            words = re.findall(r"[a-zàâçéèêëîïôûùüÿñœ]+", content)
            book_word_sets[book_id] = set(words)
            
            # Initialiser le nœud dans le graphe
            self.graph[book_id] = {}
        
        print(f"{len(self.book_ids)} livres chargés")
        
        # Étape 2: Calculer les similarités Jaccard
        self.jaccard_similarities = {}
        
        for i, book1 in enumerate(self.book_ids):
            if i % 100 == 0:
                print(f"Calcul des similarités... {i}/{len(self.book_ids)}")
                
            set1 = book_word_sets[book1]
            
            for book2 in self.book_ids[i+1:]:
                set2 = book_word_sets[book2]
                
                # Calcul de la similarité Jaccard
                intersection = len(set1.intersection(set2))
                union = len(set1.union(set2))
                
                if union > 0:
                    similarity = intersection / union
                    
                    # Ajouter une arête si la similarité dépasse un seuil
                    if similarity > 0.01:  # Seuil ajustable
                        self.graph[book1][book2] = similarity
                        self.graph[book2][book1] = similarity
                        self.jaccard_similarities[(book1, book2)] = similarity
                        self.jaccard_similarities[(book2, book1)] = similarity
        
        print(f"Graphe construit avec {len(self.graph)} nœuds et {sum(len(neighbors) for neighbors in self.graph.values()) // 2} arêtes")
    
    def compute_pagerank(self, alpha=0.85, max_iter=100, tol=1e-6):
        """Calcule le PageRank pour tous les livres"""
        print("Calcul du PageRank...")
        
        if not self.graph:
            self.build_jaccard_graph()
            
        n = len(self.book_ids)
        if n == 0:
            return {}
            
        # Initialisation uniforme
        pagerank = {book_id: 1.0 / n for book_id in self.book_ids}
        
        for iteration in range(max_iter):
            new_pagerank = {}
            total_change = 0
            
            for book_id in self.book_ids:
                # Contribution des voisins
                rank_sum = 0
                for neighbor, weight in self.graph.get(book_id, {}).items():
                    # Poids normalisé par la somme des poids sortants du voisin
                    total_out_weight = sum(self.graph[neighbor].values())
                    if total_out_weight > 0:
                        rank_sum += pagerank[neighbor] * (weight / total_out_weight)
                
                # Formule PageRank
                new_rank = (1 - alpha) / n + alpha * rank_sum
                new_pagerank[book_id] = new_rank
                total_change += abs(new_rank - pagerank[book_id])
            
            pagerank = new_pagerank
            
            if total_change < tol:
                print(f"PageRank convergé après {iteration + 1} itérations")
                break
        
        # Normalisation
        total = sum(pagerank.values())
        if total > 0:
            pagerank = {k: v / total for k, v in pagerank.items()}
        
        return pagerank
    
    def compute_betweenness(self):
        """Calcule la betweenness centrality sans NetworkX"""
        print("Calcul de la betweenness centrality...")
        
        if not self.graph:
            self.build_jaccard_graph()
            
        betweenness = {book_id: 0.0 for book_id in self.book_ids}
        
        for i, source in enumerate(self.book_ids):
            if i % 50 == 0:
                print(f"Betweenness: {i}/{len(self.book_ids)}")
                
            # Structures pour BFS pondéré
            pred = {book_id: [] for book_id in self.book_ids}
            dist = {book_id: float('inf') for book_id in self.book_ids}
            sigma = {book_id: 0 for book_id in self.book_ids}
            
            dist[source] = 0
            sigma[source] = 1
            
            # File pour BFS
            queue = deque([source])
            stack = []
            
            while queue:
                current = queue.popleft()
                stack.append(current)
                
                for neighbor, weight in self.graph.get(current, {}).items():
                    # Distance = 1/poids (plus le poids est élevé, plus la distance est courte)
                    if weight > 0:
                        distance = 1.0 / weight
                        
                        if dist[neighbor] == float('inf'):
                            dist[neighbor] = dist[current] + distance
                            queue.append(neighbor)
                        
                        if dist[neighbor] == dist[current] + distance:
                            sigma[neighbor] += sigma[current]
                            pred[neighbor].append(current)
            
            # Accumulation
            delta = {book_id: 0 for book_id in self.book_ids}
            while stack:
                current = stack.pop()
                for pred_node in pred[current]:
                    delta[pred_node] += (sigma[pred_node] / sigma[current]) * (1 + delta[current])
                if current != source:
                    betweenness[current] += delta[current]
        
        # Normalisation pour un graphe non orienté
        n = len(self.book_ids)
        if n > 2:
            betweenness = {k: v / ((n - 1) * (n - 2)) for k, v in betweenness.items()}
        
        return betweenness
    
    def compute_closeness(self):
        """Calcule la closeness centrality sans NetworkX"""
        print("Calcul de la closeness centrality...")
        
        if not self.graph:
            self.build_jaccard_graph()
            
        closeness = {}
        
        for i, source in enumerate(self.book_ids):
            if i % 50 == 0:
                print(f"Closeness: {i}/{len(self.book_ids)}")
                
            # Dijkstra pour les distances les plus courtes
            dist = {book_id: float('inf') for book_id in self.book_ids}
            dist[source] = 0
            visited = set()
            
            while len(visited) < len(self.book_ids):
                # Trouver le nœud non visité avec la distance minimale
                current = None
                min_dist = float('inf')
                for book_id in self.book_ids:
                    if book_id not in visited and dist[book_id] < min_dist:
                        min_dist = dist[book_id]
                        current = book_id
                
                if current is None:
                    break
                    
                visited.add(current)
                
                # Mettre à jour les distances des voisins
                for neighbor, weight in self.graph.get(current, {}).items():
                    if weight > 0:
                        distance = 1.0 / weight  # Convertir similarité en distance
                        new_dist = dist[current] + distance
                        if new_dist < dist[neighbor]:
                            dist[neighbor] = new_dist
            
            # Calcul de la closeness
            total_distance = sum(d for d in dist.values() if d != float('inf'))
            reachable_nodes = sum(1 for d in dist.values() if d != float('inf'))
            
            if total_distance > 0 and reachable_nodes > 1:
                closeness[source] = (reachable_nodes - 1) / total_distance
            else:
                closeness[source] = 0
        
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
    """Initialise le graphe (à appeler une fois au démarrage de l'application)"""
    try:
        book_graph.build_jaccard_graph()
        print("Graphe Jaccard initialisé avec succès")
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
                self.style.SUCCESS('Graphe initialisé avec succès!')
            )
        else:
            self.stdout.write(
                self.style.ERROR('Erreur lors de l\'initialisation du graphe')
            )