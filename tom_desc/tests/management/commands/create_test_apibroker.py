from django.core.management.base import BaseCommand
from django.contrib.auth.models import User, Group, Permission

class Command(BaseCommand):
    help = "Create api broker user for testing framework"

    def handle( self, *args, **options ):
        user = User.objects.create_user( 'apibroker', 'devnull@localhost', 'testing' )
        user.save()
        group = Group.objects.create( name="Broker" )
        group.user_set.add( user )
        group.save()
        perm = Permission.objects.get( codename='elasticc_broker' )
        group.permissions.add( perm )
