import os
from collections import defaultdict
from django.shortcuts import render
from django.conf import settings
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from .regex_index import search_regex_in_index
from django.http import Http404
import time
import csv

# Import depuis management/commands
try:
    from .management.commands.init_graph import book_graph
except ImportError:
    import sys
    from pathlib import Path
    commands_path = Path(__file__).parent / 'management' / 'commands'
    sys.path.append(str(commands_path))
    from init_graph import book_graph

# --- Connexion à Elasticsearch ---
es = Elasticsearch("http://localhost:9200", timeout=60)

# --- Charger les scores et le graphe depuis ES au démarrage du serveur ---
print("Chargement des scores depuis Elasticsearch...")
CLOSENESS_SCORES = book_graph.load_scores_from_es()
if CLOSENESS_SCORES:
    print(f"Scores chargés pour {len(CLOSENESS_SCORES)} livres")
else:
    print("Scores non trouvés. Lancez 'python manage.py init_graph' d'abord.")

print("Chargement du graphe Jaccard depuis Elasticsearch...")
GRAPH_LOADED = book_graph.load_graph_from_es()
if GRAPH_LOADED:
    print(f" Graphe Jaccard chargé: {len(book_graph.graph)} livres")
else:
    print(" Graphe Jaccard non trouvé. Lancez 'python manage.py init_graph' d'abord.")

# --- Répertoire des livres ---
BOOKS_DIR = os.path.join(settings.BASE_DIR, "moteur", "books", "gutendex_books")

# --- Mapping ID → titre ---
def build_book_info():
    info = {}
    for filename in os.listdir(BOOKS_DIR):
        if filename.endswith(".txt"):
            book_id, title = filename.split("_", 1)
            title = title.replace(".txt", "")
            info[book_id] = title
    return info

BOOK_INFO = build_book_info()

def display_book(request, book_id):
    for filename in os.listdir(BOOKS_DIR):
        if filename.startswith(str(book_id) + "_") and filename.endswith(".txt"):
            filepath = os.path.join(BOOKS_DIR, filename)
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
            return render(request, "book_view.html", {
                "book_id": book_id,
                "content": content,
                "title": filename.replace(".txt", "").split("_", 1)[1]
            })
    raise Http404("Livre introuvable")

# --- ALGORITHME KMP ---
def kmp_search(pattern, text):
    """Algorithme KMP pour recherche de motif - Complexité O(n+m)"""
    n, m = len(text), len(pattern)
    if m == 0: 
        return False
    if m > n: 
        return False
    
    # Précalcul du tableau LPS (Longest Prefix Suffix)
    lps = [0] * m
    j = 0
    for i in range(1, m):
        while j > 0 and pattern[i] != pattern[j]:
            j = lps[j-1]
        if pattern[i] == pattern[j]:
            j += 1
            lps[i] = j
    
    # Recherche du motif dans le texte
    j = 0
    for i in range(n):
        while j > 0 and text[i] != pattern[j]:
            j = lps[j-1]
        if text[i] == pattern[j]:
            j += 1
        if j == m:
            return True
    return False

def search_keyword_in_es(keyword):
    """Recherche un mot-clé exact dans l'index ES (gère les parties multiples)"""
    keyword_lower = keyword.lower()
    resp = es.search(
        index="books_index",
        body={
            "query": {
                "term": {
                    "term": keyword_lower
                }
            }
        },
        size=100
    )
    all_books = {}
    for hit in resp["hits"]["hits"]:
        books = hit["_source"]["books"]
        for book_id, count in books.items():
            all_books[book_id] = all_books.get(book_id, 0) + count

    results = [
        {
            "id": book_id,
            "title": BOOK_INFO.get(book_id, f"Livre {book_id}"),
            "count": count
        }
        for book_id, count in all_books.items()
    ]
    return results

def search_keyword_kmp(keyword):
    """Recherche un mot-clé avec KMP dans l'index ES (recherche partielle)"""
    keyword_lower = keyword.lower()
    print(f"Recherche KMP pour: '{keyword_lower}'")
    
    # ⭐⭐ REMPLACÉ es.search() par scan() pour TOUS les documents ⭐⭐
    resp_iter = scan(
        client=es,
        index="books_index",
        query={"query": {"match_all": {}}},
        preserve_order=False,
        clear_scroll=True,
        request_timeout=120
    )
    
    matching_terms = {}
    total_terms_checked = 0
    matches_found = 0
    
    for hit in resp_iter:
        term = hit["_source"]["term"]
        books = hit["_source"]["books"]
        total_terms_checked += 1
        
        # Utilise KMP pour la recherche partielle
        if kmp_search(keyword_lower, term):
            matches_found += 1
            for book_id, count in books.items():
                matching_terms[book_id] = matching_terms.get(book_id, 0) + count
    
    print(f"KMP: {matches_found} termes trouvés sur {total_terms_checked} vérifiés")
    
    results = [
        {
            "id": book_id,
            "title": BOOK_INFO.get(book_id, f"Livre {book_id}"),
            "count": count
        }
        for book_id, count in matching_terms.items()
    ]
    
    return results

def search_keyword_optimized(keyword):
    """Recherche hybride : d'abord exacte, puis KMP si peu de résultats"""
    keyword_lower = keyword.lower()
    
    # 1. Essai recherche exacte (ultra rapide)
    print(f"Recherche exacte pour: '{keyword_lower}'")
    exact_results = search_keyword_in_es(keyword_lower)
    print(f"Recherche exacte: {len(exact_results)} résultats")
    
    # Si suffisamment de résultats, on s'arrête là
    if len(exact_results) >= 8:
        return exact_results
    
    # 2. Si pas assez de résultats, utilise KMP pour recherche partielle
    print(f"Pas assez de résultats → Lancement recherche KMP...")
    kmp_results = search_keyword_kmp(keyword_lower)
    
    # Combine et déduplique les résultats
    all_results = {}
    for result in exact_results:
        all_results[result["id"]] = result
    
    for result in kmp_results:
        book_id = result["id"]
        if book_id in all_results:
            # Si le livre était déjà dans les résultats exacts, on additionne les counts
            all_results[book_id]["count"] += result["count"]
        else:
            all_results[book_id] = result
    
    final_results = list(all_results.values())
    print(f"Résultats combinés: {len(final_results)} livres")
    
    return final_results

def search_regex_in_es(pattern):
    """Recherche regex sur l'index inversé (gère les parties multiples)"""
    # ⭐⭐ REMPLACÉ es.search() par scan() pour TOUS les documents ⭐⭐
    resp_iter = scan(
        client=es,
        index="books_index",
        query={"query": {"match_all": {}}},
        preserve_order=False,
        clear_scroll=True,
        request_timeout=120
    )
    
    temp_index = {}
    for hit in resp_iter:
        term = hit["_source"]["term"]
        books = hit["_source"]["books"]
        if term in temp_index:
            for book_id, count in books.items():
                temp_index[term][book_id] = temp_index[term].get(book_id, 0) + count
        else:
            temp_index[term] = dict(books)

    results = search_regex_in_index(pattern, temp_index)
    for entry in results:
        book_id = str(entry["id"])
        entry["title"] = BOOK_INFO.get(book_id, f"Livre {book_id}")
    return results

def rank_by_occurrence(results):
    """Classe les résultats par nombre d'occurrences"""
    return sorted(results, key=lambda x: x["count"], reverse=True)

def rank_by_closeness(results):
    """Classe les résultats par closeness (charge depuis ES)"""
    if CLOSENESS_SCORES is None:
        print("Scores Closeness non disponibles")
        return rank_by_occurrence(results)
    
    ranked_results = []
    for result in results:
        book_id = str(result["id"])
        closeness = CLOSENESS_SCORES.get(book_id, 0)
        ranked_results.append({
            **result,
            "centrality_score": closeness,
            "score_type": "Closeness"
        })
    return sorted(ranked_results, key=lambda x: x["centrality_score"], reverse=True)

# --- FONCTION POUR LES SUGGESTIONS ---
def get_suggestions_from_results(results):
    """Récupère les suggestions basées sur les 3 premiers résultats"""
    if not results:
        return []
    
    # Vérifie si le graphe est chargé
    if not hasattr(book_graph, 'graph') or not book_graph.graph:
        print("Graphe Jaccard non chargé")
        return []
    
    # Prend les 3 premiers résultats
    top_3_books = [str(book["id"]) for book in results[:3]]
    print(f"Recherche suggestions pour: {top_3_books}")
    
    suggestions_resultat = []
    
    # Cherche les voisins de ces 3 livres dans le graphe Jaccard
    for book_id in top_3_books:
        if book_id in book_graph.graph:
            neighbors = book_graph.graph[book_id]
            for neighbor_id, similarity in neighbors.items():
                # Évite de suggérer les livres déjà dans les résultats
                if neighbor_id not in top_3_books:
                    suggestions_resultat.append({
                        "id": neighbor_id,
                        "title": BOOK_INFO.get(neighbor_id, f"Livre {neighbor_id}"),
                        "similarity": similarity
                    })
    
    # Supprime les doublons et garde les meilleures suggestions
    unique_suggestions = {}
    for suggestion in suggestions_resultat:
        book_id = suggestion["id"]
        if book_id not in unique_suggestions or suggestion["similarity"] > unique_suggestions[book_id]["similarity"]:
            unique_suggestions[book_id] = suggestion
    
    # Trie par similarité et prend les 5 meilleures
    sorted_suggestions = sorted(unique_suggestions.values(), key=lambda x: x["similarity"], reverse=True)[:5]
    
    print(f"{len(sorted_suggestions)} suggestions générées")
    return sorted_suggestions

# --- View principale ---
def index(request):

    start_time_total = time.time()  # Temps total

    results_index = []
    results_regex = []
    suggestions_resultat = []  # Stocke les suggestions
    keyword_index = ""
    regex_query = ""
    ranking_method = "occurrence"
    search_time = 0
    regex_time = 0 
    total_time=0

    if request.method == "POST":
        ranking_method = request.POST.get("ranking_method", "occurrence")
        keyword_index = request.POST.get("mot", "").strip()
        regex_query = request.POST.get("regex", "").strip()

        # ----- Recherche mot-clé exact -----
        if keyword_index:
            print(f"🔍 Recherche du mot: {keyword_index}")
            try:
                t0 = time.time()
                # UTILISE LA NOUVELLE FONCTION AVEC KMP
                raw_results = search_keyword_optimized(keyword_index)
                search_time = time.time() - t0   # ← Temps de recherche

                print(f"Résultats trouvés: {len(raw_results)}")
                
                if ranking_method == "occurrence":
                    results_index = rank_by_occurrence(raw_results)
                elif ranking_method == "closeness":
                    results_index = rank_by_closeness(raw_results)
                
                # Génère les suggestions
                suggestions_resultat = get_suggestions_from_results(results_index)
                print(f" {len(suggestions_resultat)} suggestions générées")
                
            except Exception as e:
                print(f" Erreur recherche mot-clé: {e}")
                results_index = []

        # ----- Recherche regex -----
        if regex_query:
            print(f" Recherche regex: {regex_query}")
            try:
                t0 = time.time()
                raw_regex_results = search_regex_in_es(regex_query)
                regex_time = time.time() - t0
                print(f"Résultats regex trouvés: {len(raw_regex_results)}")
                
                if ranking_method == "occurrence":
                    results_regex = rank_by_occurrence(raw_regex_results)
                elif ranking_method == "closeness":
                    results_regex = rank_by_closeness(raw_regex_results)
                
                # Génère les suggestions (si pas déjà fait par la recherche mot-clé)
                if not suggestions_resultat:
                    suggestions_resultat = get_suggestions_from_results(results_regex)
                    print(f" {len(suggestions_resultat)} suggestions regex générées")
                    
            except Exception as e:
                print(f"Erreur regex: {e}")
                results_regex = []

        total_time = time.time() - start_time_total


    #sauvegarde des performances    
    log_path = os.path.join(settings.BASE_DIR, "performance_log.csv")

    with open(log_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([keyword_index, regex_query, search_time, regex_time, total_time])

    return render(request, "searchapp/index.html", {
        "results_index": results_index,
        "results_regex": results_regex,
        "suggestions_resultat": suggestions_resultat,  # Passe les suggestions au template
        "keyword_index": keyword_index,
        "regex_query": regex_query,
        "ranking_method": ranking_method,
        "graph_stats": book_graph.get_graph_stats()
    })