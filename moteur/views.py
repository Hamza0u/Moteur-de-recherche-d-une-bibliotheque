from .regex_index import search_regex_in_index
import json
import os
from django.shortcuts import render
from django.conf import settings

# chemins
INDEX_FILE = os.path.join(settings.BASE_DIR, "moteur","books" , "index.json")
BOOKS_DIR = os.path.join(settings.BASE_DIR, "moteur", "books", "gutendex_books")

# charger l'index
with open(INDEX_FILE, "r", encoding="utf-8") as f:
    INDEX = json.load(f)

# recherche texte intégral
def search_books(keyword):
    results = []
    keyword_lower = keyword.lower()
    for filename in os.listdir(BOOKS_DIR):
        if filename.endswith(".txt"):
            filepath = os.path.join(BOOKS_DIR, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
                if keyword_lower in content.lower():
                    results.append(filename)
    return results

# page principale
def index(request):
    results_books = []
    results_index = []
    results_regex = []
    keyword_books = ""
    keyword_index = ""
    regex_query = ""

    if request.method == "POST":
        # recherche classique
        keyword_books = request.POST.get("keyword", "").strip()
        if keyword_books:
            results_books = search_books(keyword_books)
        
        # recherche avec index
        keyword_index = request.POST.get("mot", "").strip().lower()
        if keyword_index in INDEX:
            for book_id, count in INDEX[keyword_index].items():
                results_index.append({"id": book_id, "count": count})
            results_index = sorted(results_index, key=lambda x: x["count"], reverse=True)

        regex_query = request.POST.get("regex", "").strip()
        if regex_query:
            try:
                results_regex = search_regex_in_index(regex_query, INDEX)
                id_to_filename = {}
                for filename in os.listdir(BOOKS_DIR):
                    if filename.endswith(".txt"):
                        book_id_part = filename.split("_", 1)[0]
                        id_to_filename[book_id_part] = filename

                # Ajouter le filename à chaque entrée
                for entry in results_regex:
                    entry_id_str = str(entry["id"])
                    entry["filename"] = id_to_filename.get(entry_id_str, entry_id_str)
            except Exception:
                results_regex = []

    return render(request, "searchapp/index.html", {
        "results_books": results_books,
        "results_index": results_index,
        "results_regex": results_regex,
        "keyword_books": keyword_books,
        "keyword_index": keyword_index,
        "regex_query": regex_query,
    })
