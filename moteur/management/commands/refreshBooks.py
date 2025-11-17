import os
import time
from django.core.management.base import BaseCommand
from django.conf import settings
from moteur.models import Book

class Command(BaseCommand):
    help = "Crée ou met à jour les instances Book à partir des fichiers .txt téléchargés."

    def handle(self, *args, **options):
        books_dir = os.path.join(settings.BASE_DIR, "moteur", "books", "gutendex_books")

        if not os.path.isdir(books_dir):
            raise Exception(f"Le dossier des livres n'existe pas : {books_dir}")

        self.stdout.write(f'[{time.ctime()}] Début de la création des Book depuis {books_dir}...')

        created_count = 0
        updated_count = 0

        for filename in os.listdir(books_dir):
            if not filename.endswith(".txt"):
                continue
            filepath = os.path.join(books_dir, filename)
            parts = filename.split("_", 1)
            if len(parts) != 2:
                self.stdout.write(self.style.WARNING(f"Nom de fichier inattendu : {filename}"))
                continue

            try:
                gutenberg_id = int(parts[0])
            except ValueError:
                self.stdout.write(self.style.WARNING(f"Impossible lire id dans : {filename}"))
                continue

            title_part = parts[1].rsplit(".txt", 1)[0]
            title = title_part

            # Lire le contenu
            with open(filepath, "r", encoding="utf-8") as f:
                text = f.read()

            word_count = len(text.split())

            # données inconnues pour l’instant
            authors = ""
            language = "unk"

            book, created = Book.objects.update_or_create(
                gutenberg_id=gutenberg_id,
                defaults={
                    "filename": filename,
                    "title": title,
                    "authors": authors,
                    "language": language,
                    "word_count": word_count,
                    "content": text,            
                },
            )

            if created:
                created_count += 1
                self.stdout.write(self.style.SUCCESS(
                    f'[{time.ctime()}] Book créé : id={gutenberg_id}, fichier="{filename}"'
                ))
            else:
                updated_count += 1
                self.stdout.write(
                    f'[{time.ctime()}] Book mis à jour : id={gutenberg_id}, fichier="{filename}"'
                )

        self.stdout.write(f'[{time.ctime()}] Terminé. {created_count} créés, {updated_count} mis à jour.')
