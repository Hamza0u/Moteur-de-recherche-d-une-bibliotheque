import requests
import time
import re
import os

OUTPUT_DIR = "gutendex_books"
os.makedirs(OUTPUT_DIR, exist_ok=True)

MAX_FILENAME_LEN = 150
RETRIES = 5
TIMEOUT = 30  # secondes

def get_books_page(page_url=None):
    url = page_url if page_url else "https://gutendex.com/books/"
    for attempt in range(RETRIES):
        try:
            resp = requests.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.RequestException, requests.exceptions.ChunkedEncodingError) as e:
            print(f"Erreur téléchargement page {url}: {e} (tentative {attempt+1}/{RETRIES})")
            time.sleep(2)
    raise Exception(f"Impossible de récupérer la page {url} après {RETRIES} tentatives")

def download_plain_text(formats):
    for fmt, u in formats.items():
        if fmt.startswith("text/plain"):
            for attempt in range(RETRIES):
                try:
                    resp = requests.get(u, timeout=TIMEOUT)
                    resp.raise_for_status()
                    return resp.text
                except (requests.exceptions.RequestException, requests.exceptions.ChunkedEncodingError) as e:
                    print(f"Erreur téléchargement {u}: {e} (tentative {attempt+1}/{RETRIES})")
                    time.sleep(2)
    return None

def sanitize_filename(name):
    name = re.sub(r'[\\/*?:"<>|]', "_", name)
    if len(name) > MAX_FILENAME_LEN:
        name = name[:MAX_FILENAME_LEN]
    return name

def count_words(text):
    return len(text.split())

def save_book_txt(book_id, title, text):
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

        time.sleep(0.5)

    print(f"\nTerminé ! Total livres sauvegardés : {found_books_count}")

if __name__ == "__main__":
    main()
