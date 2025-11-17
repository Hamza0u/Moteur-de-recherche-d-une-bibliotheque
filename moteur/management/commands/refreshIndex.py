import os
import time
import json
from django.core.management.base import BaseCommand
from django.conf import settings
from moteur.models import Book, InvertedIndexEntry

class Command(BaseCommand):
    help = "Crée les entrées d'index inversé (InvertedIndexEntry) à partir de index.json."

    def handle(self, *args, **options):
        index_file = os.path.join(settings.BASE_DIR, "moteur", "books", "index.json")

        if not os.path.isfile(index_file):
            raise Exception(f"Le fichier index.json n'existe pas : {index_file}")

        self.stdout.write(f'[{time.ctime()}] Chargement de {index_file}...')
        with open(index_file, "r", encoding="utf-8") as f:
            index_data = json.load(f)

        self.stdout.write(f'[{time.ctime()}] Suppression des anciennes entrées d\'index...')
        InvertedIndexEntry.objects.all().delete()

        created_count = 0
        skipped_books = 0

        self.stdout.write(f'[{time.ctime()}] Création des nouvelles entrées d\'index...')

        for term, postings in index_data.items():
            for book_id_str, count in postings.items():
                try:
                    gutenberg_id = int(book_id_str)
                except ValueError:
                    self.stdout.write(self.style.WARNING(
                        f"ID de livre non entier dans index.json : {book_id_str} (mot='{term}')"
                    ))
                    continue

                try:
                    book = Book.objects.get(gutenberg_id=gutenberg_id)
                except Book.DoesNotExist:
                    skipped_books += 1
                    # self.stdout.write(self.style.WARNING(
                    #     f"Aucun Book avec gutenberg_id={gutenberg_id}, mot='{term}' ignoré."
                    # ))
                    continue

                InvertedIndexEntry.objects.create(
                    term=term,
                    book=book,
                    count=count,
                )
                created_count += 1

                if created_count % 10000 == 0:
                    self.stdout.write(
                        f'[{time.ctime()}] {created_count} entrées d\'index créées...'
                    )

        self.stdout.write(self.style.SUCCESS(
            f'[{time.ctime()}] Terminé. {created_count} entrées créées.'
        ))
        if skipped_books:
            self.stdout.write(
                f"{skipped_books} entrées d'index ignorées (aucun Book correspondant en base)."
            )
