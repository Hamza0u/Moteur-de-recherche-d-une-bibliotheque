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
        es = Elasticsearch("http://localhost:9200", timeout=60)

        # ───────────────────────────────────────────────
        # 1) SUPPRIMER l'ancien index et en créer un nouveau
        # ───────────────────────────────────────────────
        if es.indices.exists(index="books_index"):
            es.indices.delete(index="books_index")
            self.stdout.write(self.style.WARNING("🗑️  Ancien index supprimé"))

        es.indices.create(index="books_index", body={
            "settings": {
                "index": {
                    "max_result_window": 50000
                }
            },
            "mappings": {
                "properties": {
                    "term": {"type": "keyword"},
                    "part": {"type": "integer"},
                    "books": {"type": "flattened"}
                }
            }
        })
        self.stdout.write(self.style.SUCCESS("✅ Index ES créé : books_index"))

        # ───────────────────────────────────────────────
        # 2) Construire l'index inversé COMPLET en mémoire
        # ───────────────────────────────────────────────
        books_dir = os.path.join(settings.BASE_DIR, "moteur", "books", "gutendex_books")
        
        inverted = defaultdict(lambda: defaultdict(int))
        count = 0

        self.stdout.write("📚 Lecture des livres...")

        for filename in os.listdir(books_dir):
            if not filename.endswith(".txt"):
                continue

            match = re.match(r"(\d+)", filename)
            if not match:
                continue

            book_id = match.group(1)
            path = os.path.join(books_dir, filename)
            
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read().lower()

            # ⬅️ AUCUN FILTRE : tous les mots sont pris
            words = re.findall(r"[a-zàâçéèêëîïôûùüÿñœ]+", text)

            for w in words:
                inverted[w][book_id] += 1  # ⬅️ PAS DE if len(w) >= 4

            count += 1
            if count % 100 == 0:
                self.stdout.write(f"  📖 {count} livres lus...")

        self.stdout.write(self.style.SUCCESS(f"✅ {count} livres traités, {len(inverted)} mots uniques trouvés"))

        # ───────────────────────────────────────────────
        # 3) Envoyer à ES en découpant les gros mots
        # ───────────────────────────────────────────────
        self.stdout.write("📤 Envoi à Elasticsearch...")
        
        MAX_BOOKS_PER_DOC = 500  # Maximum 500 livres par document
        
        actions = []
        batch_count = 0
        split_terms = []
        
        for term, postings in inverted.items():
            postings_list = list(postings.items())
            nb_books = len(postings_list)
            
            # Si le mot est dans beaucoup de livres, on découpe
            if nb_books > MAX_BOOKS_PER_DOC:
                split_terms.append((term, nb_books))
                
                # Découper en chunks de MAX_BOOKS_PER_DOC livres
                for part_num, i in enumerate(range(0, nb_books, MAX_BOOKS_PER_DOC)):
                    chunk = dict(postings_list[i:i + MAX_BOOKS_PER_DOC])
                    
                    actions.append({
                        "_index": "books_index",
                        "_id": f"{term}_part_{part_num}",
                        "_source": {
                            "term": term,
                            "part": part_num,
                            "books": chunk
                        }
                    })
            else:
                # Mot normal, un seul document
                actions.append({
                    "_index": "books_index",
                    "_id": term,
                    "_source": {
                        "term": term,
                        "part": 0,
                        "books": dict(postings)
                    }
                })
            
            # Envoyer par batches de 1000 documents
            if len(actions) >= 1000:
                success, errors = bulk(
                    es, 
                    actions, 
                    raise_on_error=False,
                    request_timeout=60
                )
                batch_count += success
                
                if errors:
                    self.stdout.write(self.style.WARNING(f"⚠️  {len(errors)} erreurs"))
                
                self.stdout.write(f"  ✓ {batch_count} documents indexés...")
                actions = []

        # Envoyer le reste
        if actions:
            success, errors = bulk(
                es, 
                actions, 
                raise_on_error=False,
                request_timeout=60
            )
            batch_count += success

        self.stdout.write(self.style.SUCCESS(
            f"🎉 Indexation terminée : {batch_count} documents indexés !"
        ))
        
        if split_terms:
            self.stdout.write(self.style.WARNING(
                f"\nℹ️  {len(split_terms)} mots découpés en plusieurs parties:"
            ))
            for term, nb_books in sorted(split_terms, key=lambda x: x[1], reverse=True)[:10]:
                nb_parts = (nb_books + MAX_BOOKS_PER_DOC - 1) // MAX_BOOKS_PER_DOC
                self.stdout.write(f"  • '{term}' : {nb_books} livres → {nb_parts} parties")