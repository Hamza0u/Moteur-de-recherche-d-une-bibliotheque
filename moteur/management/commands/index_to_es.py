import os
import re
from collections import defaultdict
from django.core.management.base import BaseCommand
from django.conf import settings
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class Command(BaseCommand):
    help = "Construit un index inversé et l'envoie dans Elasticsearch"

    def handle(self, *args, **kwargs):

        es = Elasticsearch("http://localhost:9200")

        # ───────────────────────────────────────────────
        # 1) Créer l'index ES pour accueillir { mot → livres }
        # ───────────────────────────────────────────────
        if not es.indices.exists(index="inverted_index"):
            es.indices.create(index="inverted_index", body={
                "mappings": {
                    "properties": {
                        "term": {"type": "keyword"},
                        "books": {"type": "object"}   # {book_id: freq}
                    }
                }
            })
            self.stdout.write(self.style.SUCCESS("Index ES créé : inverted_index"))

        # ───────────────────────────────────────────────
        # 2) Construire l'index inversé en Python
        # ───────────────────────────────────────────────
        books_dir = os.path.join(settings.BASE_DIR, "moteur", "books", "gutendex_books")
        inverted = defaultdict(lambda: defaultdict(int))

        count = 0

        for filename in os.listdir(books_dir):
            if not filename.endswith(".txt"):
                continue

            match = re.match(r"(\d+)", filename)
            if not match:
                continue

            book_id = int(match.group(1))

            path = os.path.join(books_dir, filename)
            text = open(path, "r", encoding="utf-8", errors="ignore").read().lower()

            words = re.findall(r"[a-zàâçéèêëîïôûùüÿñœ]+", text)

            for w in words:
                inverted[w][book_id] += 1

            count += 1
            if count % 50 == 0:
                self.stdout.write(f"{count} livres traités...")

        # ───────────────────────────────────────────────
        # 3) Préparer le BATCH bulk pour Elasticsearch
        # ───────────────────────────────────────────────
        actions = []
        for term, postings in inverted.items():
            actions.append({
                "_index": "inverted_index",
                "_id": term,
                "_source": {
                    "term": term,
                    "books": postings
                }
            })

        bulk(es, actions)

        self.stdout.write(self.style.SUCCESS(
            f"Index inversé envoyé à ES : {len(inverted)} mots indexés."
        ))
