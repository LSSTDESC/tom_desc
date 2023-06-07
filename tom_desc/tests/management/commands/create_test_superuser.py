from django.core.management.base import BaseCommand
from django.contrib.auth.models import User

class Command(BaseCommand):
    help = "Create insecure superuser for testing framework"

    def handle( self, *args, **options ):
        admin = User.objects.create_superuser( 'root', 'devull@localhost', 'testing' )
