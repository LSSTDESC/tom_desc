"""
WSGI config for the DESC TOM.  Based on the dockertom.

It exposes the WSGI callable as a module-level variable named ``application``.
"""

import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tom_desc.settings')

application = get_wsgi_application()
