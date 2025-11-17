from django.db import models

class Book(models.Model):
    # identifiant du livre (ex : 11, 84, 1342, etc.)
    gutenberg_id = models.IntegerField(unique=True)

    # nom de fichier sur disque, ex : "11_Alice's Adventures in Wonderland.txt"
    filename = models.CharField(max_length=300, unique=True)

    title = models.CharField(max_length=500)
    authors = models.CharField(max_length=500, blank=True)   # optionnel
    language = models.CharField(max_length=10)               # 'en', 'fr', etc.
    word_count = models.PositiveIntegerField()
    content = models.TextField(blank=True)  # à toi de voir si tu le remplis

    def __str__(self):
        return f"{self.gutenberg_id} - {self.title} ({self.language})"

class InvertedIndexEntry(models.Model):
    """
    Entrée de l'index inversé :
    - term : le mot (ex : "alice")
    - book : référence vers un Book
    - count : nb d'occurrences du mot dans ce livre
    """
    term = models.CharField(max_length=100, db_index=True)
    book = models.ForeignKey(Book, on_delete=models.CASCADE, related_name="index_entries")
    count = models.PositiveIntegerField()

    class Meta:
        unique_together = ('term', 'book')
        indexes = [
            models.Index(fields=['term']),
            models.Index(fields=['book']),
        ]

    def __str__(self):
        return f"{self.term} -> {self.book.filename} ({self.count})"
