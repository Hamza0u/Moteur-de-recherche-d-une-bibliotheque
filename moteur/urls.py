from django.urls import path
from . import views

urlpatterns = [
    # Page principale : formulaire + résultats
    path('', views.index, name='search_index'),
    path("book/<int:book_id>/", views.display_book, name="display_book"),
]
