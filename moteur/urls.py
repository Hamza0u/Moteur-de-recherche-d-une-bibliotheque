from django.urls import path
from . import views

urlpatterns = [
    # Page principale : formulaire + résultats
    path('', views.index, name='search_index'),
]
