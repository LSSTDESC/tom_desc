from django.core.management.base import BaseCommand
from django.contrib.auth.models import User, Group

class Command(BaseCommand):
    help = "Create insecure superuser for testing framework"

    def handle( self, *args, **options ):
        group = Group.objects.create( name="Public" )
        group.user_set.add(*User.objects.all())
        group.save()
        admin = User.objects.create_superuser( 'root', 'devull@localhost', 'testing' )
