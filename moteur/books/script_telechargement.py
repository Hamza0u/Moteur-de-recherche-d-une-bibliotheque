import requests
import json
import time
import re
import os

# Dossier où on sauvegarde les fichiers
OUTPUT_DIR = "."
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_books_page(page_url=None):
    """Récupère une page de livres depuis Gutendex."""
    url = page_url if page_url else "https://gutendex.com/books/"
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()

def download_plain_text(formats):
    """Télécharge le texte brut si disponible."""
    for fmt, u in formats.items():
        if fmt.startswith("text/plain"):
            resp = requests.get(u)
            resp.raise_for_status()
            return resp.text
    return None

def sanitize_filename(name):
    """Nettoie un nom de fichier pour enlever les caractères interdits."""
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def save_book_json(book_info):
    """Sauvegarde les informations d’un livre dans un fichier JSON."""
    title_sanitized = sanitize_filename(book_info['title'])
    filename = f"{book_info['id']}_{title_sanitized}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(book_info, f, ensure_ascii=False, indent=2)
    print(f"-> Livre sauvegardé : {filename}")

def main():
    found_books_count = 0
    next_page = None

    while found_books_count < 1664:
        data = get_books_page(next_page)
        for book in data['results']:
            formats = book.get('formats', {})
            text = download_plain_text(formats)
            if text and len(text) > 10000:
                book_info = {
                    'id': book['id'],
                    'title': book['title'],
                    'authors': [a['name'] for a in book.get('authors', [])],
                    'length': len(text),
                    'text': text
                }
                save_book_json(book_info)
                found_books_count += 1
                print(f"[{found_books_count}/1664] {book['title']} ({len(text)} chars)")

            if found_books_count >= 1664:
                break

        next_page = data.get('next')
        if not next_page:
            print("Plus de pages disponibles dans l'API.")
            break

        time.sleep(0.5)  # Respect de l'API

    print(f"\nTerminé ! Total livres sauvegardés : {found_books_count}")

if __name__ == "__main__":
    main()
