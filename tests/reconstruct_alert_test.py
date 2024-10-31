import sys
import time
import logging

from elasticc2.models import PPDBAlert

_logger = logging.getLogger("main")
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_logout.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                         datefmt='%Y-%m-%d %H:%M:%S' ) )
_logger.propagate = False
_logger.setLevel( logging.INFO )

def test_reconstruct_alerts( elasticc2_ppdb ):
    alerts = list( PPDBAlert.objects.all() )

    t0 = time.perf_counter()
    for alert in alerts:
        it = alert.reconstruct( debug=True )
    t1 = time.perf_counter()

    _logger.info( f"{t1-t0:.1f} sec to reconstruct {len(alerts)} alerts ({len(alerts)/(t1-t0):.0f} s⁻¹)" )

    import pdb; pdb.set_trace()
    pass
