import os
from django.shortcuts import render
from django.conf import settings
from elasticsearch import Elasticsearch
from .regex_index import search_regex_in_index  # ton moteur regex existant

# --- Connexion à Elasticsearch --- #
es = Elasticsearch("http://localhost:9200")

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


# --- Fonction de recherche regex sur ES --- #
def search_regex_in_es(pattern):
    """Utilise ton moteur regex local sur l'index inversé stocké dans ES."""
    # Récupérer tous les termes
    resp = es.search(
        index="inverse_index",
        body={"query": {"match_all": {}}},
        size=10000  # adapter si plus de termes
    )
    
    # Recréer un dict pour le moteur regex
    temp_index = {hit["_source"]["term"]: hit["_source"]["books"] for hit in resp["hits"]["hits"]}
    
    results = search_regex_in_index(pattern, temp_index)
    
    # Ajouter les titres des livres
    for entry in results:
        book_id = str(entry["id"])
        entry["title"] = BOOK_INFO.get(book_id, f"Livre {book_id}")
    
    return results


# --- View principale --- #
def index(request):
    results_index = []
    results_regex = []
    keyword_index = ""
    regex_query = ""

    if request.method == "POST":
        # ----- Recherche mot-clé exact dans ES ----- #
        keyword_index = request.POST.get("mot", "").strip()
        if keyword_index:
            resp = es.search(
                index="inverse_index",
                body={
                    "query": {
                        "match": {
                            "term": keyword_index  # insensible à la casse si l’analyzer le permet
                        }
                    }
                }
            )
           
            results_index = []
            for hit in resp["hits"]["hits"]:
                books = hit["_source"]["books"]
                results_index.extend([
                    {
                        "id": book_id,
                        "title": BOOK_INFO.get(book_id, f"Livre {book_id}"),
                        "count": count
                    }
                    for book_id, count in books.items()
                ])
            results_index.sort(key=lambda x: x["count"], reverse=True)

        # ----- Recherche regex sur l'index inversé ES ----- #
        regex_query = request.POST.get("regex", "").strip()
        if regex_query:
            try:
                results_regex = search_regex_in_es(regex_query)
            except Exception:
                results_regex = []

    return render(request, "searchapp/index.html", {
        "results_index": results_index,
        "results_regex": results_regex,
        "keyword_index": keyword_index,
        "regex_query": regex_query,
    })
