# moteur/management/commands/index_books_es.py

import os
import json
from django.core.management.base import BaseCommand
from django.conf import settings
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class Command(BaseCommand):
    help = "Indexe l'index inversé dans Elasticsearch avec bulk"

    def handle(self, *args, **kwargs):
        es = Elasticsearch("http://localhost:9200")

        # Création de l'index si absent
        if not es.indices.exists(index="books_index"):
            es.indices.create(index="books_index", body={
                "settings": {
                    "analysis": {
                        "analyzer": {
                            "default": {
                                "type": "standard"
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "term": {"type": "keyword"},
                        "books": {"type": "object"}  # {book_id: count, ...}
                    }
                }
            })
            self.stdout.write(self.style.SUCCESS("Index Elasticsearch créé : books_index"))

        # Charger l'index inversé JSON
        index_file = os.path.join(settings.BASE_DIR, "moteur", "books", "index.json")
        with open(index_file, "r", encoding="utf-8") as f:
            index_data = json.load(f)

        self.stdout.write(f"{len(index_data)} termes à indexer...")

        # Préparer les actions pour bulk
        actions = []
        for i, (term, postings) in enumerate(index_data.items(), start=1):
            actions.append({
                "_index": "books_index",
                "_id": term,
                "_source": {
                    "term": term,
                    "books": postings
                }
            })
            # Afficher une progression tous les 1000 termes
            if i % 1000 == 0:
                self.stdout.write(f"{i} termes préparés...")

        # Exécuter le bulk
        bulk(es, actions)
        self.stdout.write(self.style.SUCCESS(f"Indexation terminée pour {len(actions)} termes"))
