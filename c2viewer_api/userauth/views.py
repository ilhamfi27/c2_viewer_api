from django.shortcuts import render

def index(request):
    return render(request, 'userauth/index.html')

def home(request):
    return render(request, 'userauth/home.html')
