import os
from django.shortcuts import render
from django.conf import settings
from elasticsearch import Elasticsearch
from .regex_index import search_regex_in_index
from django.http import Http404

# Import depuis management/commands
try:
    from .management.commands.init_graph import book_graph
except ImportError:
    import sys
    from pathlib import Path
    commands_path = Path(__file__).parent / 'management' / 'commands'
    sys.path.append(str(commands_path))
    from init_graph import book_graph

# --- Connexion à Elasticsearch --- #
es = Elasticsearch("http://localhost:9200", timeout=60)

# --- Charger les scores depuis ES au démarrage du serveur --- #
print("🔄 Chargement des scores depuis Elasticsearch...")
CLOSENESS_SCORES = book_graph.load_scores_from_es()

if CLOSENESS_SCORES:
    print(f"✅ Scores chargés pour {len(CLOSENESS_SCORES)} livres")
else:
    print("⚠️  Scores non trouvés. Lancez 'python manage.py init_graph' d'abord.")

# --- Répertoire des livres --- #
BOOKS_DIR = os.path.join(settings.BASE_DIR, "moteur", "books", "gutendex_books")

# --- Mapping ID → titre --- #
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

def search_regex_in_es(pattern):
    """Recherche regex sur l'index inversé (gère les parties multiples)"""
    resp = es.search(
        index="books_index",
        body={"query": {"match_all": {}}},
        size=10000
    )
    
    temp_index = {}
    for hit in resp["hits"]["hits"]:
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
        print("⚠️  Scores Closeness non disponibles")
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

# --- View principale --- #
def index(request):
    results_index = []
    results_regex = []
    keyword_index = ""
    regex_query = ""
    ranking_method = "occurrence"

    if request.method == "POST":
        ranking_method = request.POST.get("ranking_method", "occurrence")
        keyword_index = request.POST.get("mot", "").strip()
        regex_query = request.POST.get("regex", "").strip()

        # ----- Recherche mot-clé exact ----- #
        if keyword_index:
            print(f"🔍 Recherche du mot: {keyword_index}")
            
            try:
                raw_results = search_keyword_in_es(keyword_index)
                print(f"📊 Résultats trouvés: {len(raw_results)}")
                
                if ranking_method == "occurrence":
                    results_index = rank_by_occurrence(raw_results)
                elif ranking_method == "closeness":
                    results_index = rank_by_closeness(raw_results)
            except Exception as e:
                print(f"❌ Erreur recherche mot-clé: {e}")
                results_index = []

        # ----- Recherche regex ----- #
        if regex_query:
            print(f"🔍 Recherche regex: {regex_query}")
            
            try:
                raw_regex_results = search_regex_in_es(regex_query)
                print(f"📊 Résultats regex trouvés: {len(raw_regex_results)}")
                
                if ranking_method == "occurrence":
                    results_regex = rank_by_occurrence(raw_regex_results)
                elif ranking_method == "closeness":
                    results_regex = rank_by_closeness(raw_regex_results)
            except Exception as e:
                print(f"❌ Erreur regex: {e}")
                results_regex = []

    return render(request, "searchapp/index.html", {
        "results_index": results_index,
        "results_regex": results_regex,
        "keyword_index": keyword_index,
        "regex_query": regex_query,
        "ranking_method": ranking_method,
        "graph_stats": book_graph.get_graph_stats()
    })