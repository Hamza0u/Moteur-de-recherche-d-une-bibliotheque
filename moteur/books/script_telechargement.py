import requests
import time
import re
import os

# Dossier où on sauvegarde les fichiers
OUTPUT_DIR = "gutendex_books"
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

def count_words(text):
    """Compte le nombre de mots dans un texte."""
    return len(text.split())

def save_book_txt(book_id, title, text):
    """Sauvegarde un livre dans un fichier texte."""
    title_sanitized = sanitize_filename(title)
    filename = f"{book_id}_{title_sanitized}.txt"
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"-> Livre sauvegardé : {filename}")

def main():
    found_books_count = 0
    next_page = None

    while found_books_count < 1664:
        data = get_books_page(next_page)
        for book in data['results']:
            formats = book.get('formats', {})
            text = download_plain_text(formats)
            if text and count_words(text) >= 10000:
                save_book_txt(book['id'], book['title'], text)
                found_books_count += 1
                print(f"[{found_books_count}/1664] {book['title']} ({count_words(text)} mots)")

            if found_books_count >= 1664:
                break

        next_page = data.get('next')
        if not next_page:
            print("Plus de pages disponibles dans l'API.")
            break

        time.sleep(0.5)  # Pause pour respecter l'API

    print(f"\nTerminé ! Total livres sauvegardés : {found_books_count}")

if __name__ == "__main__":
    main()
