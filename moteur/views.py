import os
import json
from django.shortcuts import render
from django.conf import settings
from .regex_index import search_regex_in_index

# chemins
INDEX_FILE = os.path.join(settings.BASE_DIR, "moteur", "books", "index.json")
BOOKS_DIR = os.path.join(settings.BASE_DIR, "moteur", "books", "gutendex_books")

# --- Charger l'index JSON --- #
with open(INDEX_FILE, "r", encoding="utf-8") as f:
    INDEX = json.load(f)

# --- Construction mapping ID → titre --- #
def build_book_info():
    info = {}
    for filename in os.listdir(BOOKS_DIR):
        if filename.endswith(".txt"):
            book_id, title = filename.split("_", 1)
            title = title.replace(".txt", "")
            info[book_id] = title
    return info

BOOK_INFO = build_book_info()

# --- 🔍 Recherche texte complète --- #
def search_books(keyword):
    results = []
    keyword_lower = keyword.lower()
    for filename in os.listdir(BOOKS_DIR):
        if filename.endswith(".txt"):
            filepath = os.path.join(BOOKS_DIR, filename)
            with open(filepath, "r", encoding="utf-8") as f:
                content = f.read()
                if keyword_lower in content.lower():
                    book_id = filename.split("_", 1)[0]
                    results.append(BOOK_INFO.get(book_id, filename))
    return results


# --- 🏠 Page principale --- #
def index(request):
    results_books = []
    results_index = []
    results_regex = []
    keyword_books = ""
    keyword_index = ""
    regex_query = ""

    if request.method == "POST":

        # ----- Recherche classique ----- #
        keyword_books = request.POST.get("keyword", "").strip()
        if keyword_books:
            results_books = search_books(keyword_books)

        # ----- Recherche via index JSON ----- #
        keyword_index = request.POST.get("mot", "").strip().lower()
        if keyword_index in INDEX:
            for book_id, count in INDEX[keyword_index].items():
                results_index.append({
                    "id": book_id,
                    "title": BOOK_INFO.get(book_id, f"Livre {book_id}"),
                    "count": count
                })
            results_index = sorted(results_index, key=lambda x: x["count"], reverse=True)

        # ----- Recherche regex ----- #
        regex_query = request.POST.get("regex", "").strip()
        if regex_query:
            try:
                results_regex = search_regex_in_index(regex_query, INDEX)

                # Retrouver le nom complet du livre
                for entry in results_regex:
                    book_id = str(entry["id"])
                    entry["title"] = BOOK_INFO.get(book_id, f"Livre {book_id}")

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
