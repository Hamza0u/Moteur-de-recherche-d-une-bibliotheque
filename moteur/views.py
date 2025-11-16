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
    keyword_books = ""
    keyword_index = ""

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

    return render(request, "searchapp/index.html", {
        "results_books": results_books,
        "results_index": results_index,
        "keyword_books": keyword_books,
        "keyword_index": keyword_index
    })
