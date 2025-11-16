import os
import json
import re

BOOKS_DIR = "gutendex_books"
INDEX_FILE = "index.json"

word_pattern = re.compile(r"[a-zA-Z]+") # on va prend les mots qui on un pattern de mot "classique"


def build_index():
    index = {}

    for filename in os.listdir(BOOKS_DIR):
        if not filename.endswith(".txt"):
            continue

        book_id = filename.split("_")[0]
        path = os.path.join(BOOKS_DIR, filename)

        with open(path, "r", encoding="utf-8") as f:
            text = f.read().lower()

        words = word_pattern.findall(text)

        for w in words:
            if w not in index:
                index[w] = {} 
            if book_id not in index[w]:
                index[w][book_id] = 0
            index[w][book_id] += 1

        print(f"Indexed {filename}")

    with open(INDEX_FILE, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)

    print(f"Index saved to {INDEX_FILE}")

if __name__ == "__main__":
    build_index()
