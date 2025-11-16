from django.shortcuts import render
import os
from django.conf import settings


BOOKS_DIR = os.path.join(settings.BASE_DIR, "moteur", "books", "gutendex_books")

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

def index(request):
    results = []
    keyword = ""
    if request.method == "POST":
        keyword = request.POST.get("keyword", "").strip()
        if keyword:
            results = search_books(keyword)
    return render(request, "searchapp/index.html", {"results": results, "keyword": keyword})
