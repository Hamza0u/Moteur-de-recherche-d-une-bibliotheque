from rest_framework import serializers
from .models import Book


class BookSerializer(serializers.ModelSerializer):
    class Meta:
        model = Book
        fields = ['id', 'gutenberg_id', 'title', 'language', 'word_count']


class FrenchBookSerializer(BookSerializer):
    class Meta(BookSerializer.Meta):
        pass


class EnglishBookSerializer(BookSerializer):
    class Meta(BookSerializer.Meta):
        pass
