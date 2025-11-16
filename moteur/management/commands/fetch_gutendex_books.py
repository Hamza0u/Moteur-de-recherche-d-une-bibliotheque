# mygutenberg/management/commands/fetch_gutendex_books.py

import re
import time
import requests
from pathlib import Path

from django.core.management.base import BaseCommand

from mygutenberg.models import Book


# Dossier où on sauvegarde les .txt
LIBRARY_DIR = Path("data/library")
LIBRARY_DIR.mkdir(parents=True, exist_ok=True)

WORD_RE = re.compile(r"\w+", re.UNICODE)


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


def sanitize_filename(name: str) -> str:
    """Nettoie un nom de fichier pour enlever les caractères interdits."""
    return re.sub(r'[\\/*?:"<>|]', "_", name)


def count_words(text: str) -> int:
    return len(WORD_RE.findall(text))


class Command(BaseCommand):
    help = (
        "Télécharge des livres depuis Gutendex, "
        "sauvegarde les textes en local et crée les instances Book."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=1664,
            help="Nombre de livres à récupérer (par défaut: 1664)",
        )

    def handle(self, *args, **options):
        target = options["limit"]
        found_books_count = 0
        next_page = None

        self.stdout.write(self.style.MIGRATE_HEADING(
            f"Début de récupération des livres Gutendex (objectif: {target})"
        ))

        while found_books_count < target:
            data = get_books_page(next_page)

            for book in data["results"]:
                formats = book.get("formats", {})
                text = download_plain_text(formats)
                if not text:
                    continue

                # Compter les mots (et pas les caractères)
                n_words = count_words(text)
                if n_words < 10_000:
                    # trop court pour notre projet
                    continue

                gutenberg_id = book["id"]
                title = book["title"] or f"Book {gutenberg_id}"

                # langue (Gutendex fournit une liste de codes, ex: ["en"])
                languages = book.get("languages") or []
                language = languages[0] if languages else "unknown"

                # Nom de fichier local
                title_sanitized = sanitize_filename(title)
                filename = f"{gutenberg_id}_{title_sanitized}.txt"
                filepath = LIBRARY_DIR / filename

                # Sauvegarde du texte en .txt
                filepath.write_text(text, encoding="utf-8")

                # Création / mise à jour du Book en BDD
                obj, created = Book.objects.update_or_create(
                    gutenberg_id=gutenberg_id,
                    defaults={
                        "title": title,
                        "language": language.lower(),
                        "local_path": str(filepath),
                        "word_count": n_words,
                    },
                )

                found_books_count += 1
                status = "créé" if created else "mis à jour"
                self.stdout.write(
                    f"[{found_books_count}/{target}] {status} : "
                    f"{title} (id={gutenberg_id}, {n_words} mots, lang={language})"
                )

                if found_books_count >= target:
                    break

            next_page = data.get("next")
            if not next_page:
                self.stdout.write(self.style.WARNING(
                    "Plus de pages disponibles dans l'API Gutendex."
                ))
                break

            # Pour ne pas bourriner l'API
            time.sleep(0.5)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(
            f"Terminé ! Total livres sauvegardés en local et en BDD : {found_books_count}"
        ))
