import sys
sys.path.insert( 0, "/tom_desc/elasticc2/management/commands" )
import pittgoogle
from brokerpoll2 import PittGoogleBroker  # tom_desc/elasticc2/management/commands/brokerpoll2.py

from django.core.management.base import BaseCommand

class Command(BaseCommand):

    def handle( self, *args, **options ):
        broker = PittGoogleBroker(
            topic_name="classifications-loop",  # heartbeat topic with dummy data, for testing
            topic_project=pittgoogle.utils.ProjectIds.pittgoogle_dev,
            max_workers=8,  # max number of ThreadPoolExecutor workers
            batch_maxn=1000,  # max number of messages in a batch
            batch_maxwait=5,  # max seconds to wait between messages before processing a batch
        )

        broker.poll() # runs indefinitely. use `Ctrl-C` to stop.
