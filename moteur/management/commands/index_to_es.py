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
        # 1) Créer l'index ES
        # ───────────────────────────────────────────────
        if not es.indices.exists(index="inverted_index"):
            es.indices.create(index="inverted_index", body={
                "mappings": {
                    "properties": {
                        "term": {"type": "keyword"},
                        "books": {"type": "object"}
                    }
                }
            })
            self.stdout.write(self.style.SUCCESS("Index ES créé : inverted_index"))

        # ───────────────────────────────────────────────
        # 2) Configuration
        # ───────────────────────────────────────────────
        books_dir = os.path.join(settings.BASE_DIR, "moteur", "books", "gutendex_books")
        BATCH_SIZE = 100  # Envoyer tous les X livres
        
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
            
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read().lower()

            words = re.findall(r"[a-zàâçéèêëîïôûùüÿñœ]+", text)

            for w in words:
                if len(w) >= 4:
                    inverted[w][book_id] += 1

            count += 1

            # ───────────────────────────────────────────────
            # 3) Envoi par BATCH tous les X livres
            # ───────────────────────────────────────────────
            if count % BATCH_SIZE == 0:
                self._send_to_es(es, inverted)
                self.stdout.write(f"✓ {count} livres traités, {len(inverted)} mots envoyés")
                inverted.clear()  # VIDER la mémoire

        # ───────────────────────────────────────────────
        # 4) Envoyer le reste
        # ───────────────────────────────────────────────
        if inverted:
            self._send_to_es(es, inverted)
            self.stdout.write(f"✓ {count} livres traités (dernier batch)")

        self.stdout.write(self.style.SUCCESS(
            f"Indexation terminée : {count} livres traités."
        ))

    def _send_to_es(self, es, inverted):
        """Envoie un batch à Elasticsearch en METTANT À JOUR les postings existants"""
        actions = []
        
        for term, postings in inverted.items():
            # Récupérer les postings existants dans ES
            try:
                doc = es.get(index="inverted_index", id=term)
                existing_books = doc["_source"].get("books", {})
                
                # FUSIONNER avec les nouvelles fréquences
                for book_id, freq in postings.items():
                    existing_books[str(book_id)] = existing_books.get(str(book_id), 0) + freq
                
                actions.append({
                    "_op_type": "update",
                    "_index": "inverted_index",
                    "_id": term,
                    "doc": {"books": existing_books}
                })
            except:
                # Le terme n'existe pas encore, on le crée
                actions.append({
                    "_op_type": "index",
                    "_index": "inverted_index",
                    "_id": term,
                    "_source": {
                        "term": term,
                        "books": {str(k): v for k, v in postings.items()}
                    }
                })
        
        if actions:
            bulk(es, actions, raise_on_error=False)