import sys
import os
import io
import logging
import time
import fastavro

import django.db

from elasticc2.models import PPDBAlert

class AlertReconstructor():
    def __init__( self, parent, pipe, schemafile ):
        self._getalerttime = 0
        self._reconstructtime = 0
        self._ddfreconstructtime = 0
        self._avrowritetime = 0
        self._ddfavrowritetime = 0
        self.fullprevious = 365
        self.limitedprevious = 30

        self.pipe = pipe
        self.schema = fastavro.schema.load_schema( schemafile )

        self.logger = logging.getLogger( str( os.getpid() ) )
        self.logger.propagate = False
        logout = logging.StreamHandler( sys.stderr )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - {os.getpid()} - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        self.logger.setLevel( logging.INFO )

    def go( self ):
        done = False
        self.logger.info( f'Subprocess starting.' )
        overall_t0 = time.perf_counter()
        # An experiment.  The backend postgres processes
        # were getting big.  Maybe this was normal, but I
        # also wonder if there is a transaction leak somewhere.
        # Django being web software, you can't really trust
        # it to do the right thing for somethying long-running....
        # Try closing the postgres connection every so often.
        # (Disable this by setting deltareconnect to 0.)
        # ...this didn't seem to help
        ndone = 0
        deltareconnect = 0
        nextreconnect = 1000
        # ****
        nlongs = 0
        nlongsddf = 0
        # ****
        while not done:
            try:
                if ( deltareconnect > 0 ) and ( ndone >= nextreconnect ):
                    self.logger.info( f"Reconnecting to postgres after {ndone} alerts reconstructed" )
                    nextreconnect += deltareconnect
                    django.db.connections.close_all()

                msg = self.pipe.recv()
                if msg['command'] == 'die':
                    # self.logger.debug( f'Got die' )
                    done = True
                elif msg['command'] == 'do':
                    # self.logger.debug( f'Got do for alert dex {msg["alertdex"]}, id {msg["alert"].alert_id}' )
                    t0 = time.perf_counter()
                    alertdex = msg['alertdex']
                    alert = msg['alert']
                    # alertid = msg['alertid']
                    # alert = PPDBAlert.objects.get( pk=alertid )

                    t1 = time.perf_counter()
                    isddf = alert.diaobject.isddf
                    # self.logger.debug( "reconstructing" )
                    fullalert = alert.reconstruct( daysprevious=self.fullprevious )
                    if isddf:
                        limitedalert = alert.reconstruct( daysprevious=self.limitedprevious )
                    # self.logger.debug( "done reconstructing" )

                    t2 = time.perf_counter()

                    # ****
                    if ( nlongs < 5 ) or ( nlongsddf < 5 ):
                        if ( t2 - t1 ) > 0.03:
                            if ( not isddf ) and ( nlongs < 5 ):
                                self.logger.info( f'source {alert.diasource_id} object {alert.diaobject_id}\n'
                                                  f'took {1000*(t2-t1):.0f} ms' )
                                nlongs += 1
                            elif ( isddf ) and ( nlongsddf < 5 ):
                                self.logger.info( f'DDF source {alert.diasource_id} object {alert.diaobject_id}\n'
                                                  f'took {1000*(t2-t1):.0f} ms' )
                                nlongsddf += 1
                    # ****

                    fullmsgio = io.BytesIO()
                    fastavro.write.schemaless_writer( fullmsgio, self.schema, fullalert )
                    fullmsg = fullmsgio.getvalue()
                    if isddf:
                        limitedmsgio = io.BytesIO()
                        fastavro.write.schemaless_writer( limitedmsgio, self.schema, limitedalert )
                        limitedmsg = limitedmsgio.getvalue()
                    else:
                        limitedmsg = None
                    t3 = time.perf_counter()

                    self._getalerttime += t1 - t0
                    self._reconstructtime += t2 - t1
                    self._avrowritetime += t3 - t2
                    if isddf:
                        self._ddfreconstructtime += t2 - t1
                        self._ddfavrowritetime += t3 - t2

                    self.pipe.send( { 'response': 'alert produced',
                                      'alertdex': alertdex,
                                      'alertid': alert.alert_id,
                                      'fullhistory': fullmsg,
                                      'limitedhistory': limitedmsg
                                     } )
                else:
                    raise ValueError( f"Unknown message {msg['command']}" )

                ndone += 1
            except Exception as ex:
                # Should I be sending an error message back to the
                # parent process instead of just raising?
                raise ex

        self.pipe.send( { 'response': 'finished',
                          'tottime': time.perf_counter() - overall_t0,
                          'getalerttime': self._getalerttime,
                          'reconstructtime': self._reconstructtime,
                          'avrowritetime': self._avrowritetime,
                          'ddfreconstructtime': self._ddfreconstructtime,
                          'ddfavrowritetime': self._ddfavrowritetime,
                          'PPDBAlert._sourcetime': PPDBAlert._sourcetime,
                          'PPDBAlert._ddfsourcetime': PPDBAlert._ddfsourcetime,
                          'PPDBAlert._objectoverheadtime': PPDBAlert._objectoverheadtime,
                          'PPDBAlert._objecttime': PPDBAlert._objecttime,
                          'PPDBAlert._ddfobjecttime': PPDBAlert._ddfobjecttime,
                          'PPDBAlert._prvsourcetime': PPDBAlert._prvsourcetime,
                          'PPDBAlert._ddfprvsourcetime': PPDBAlert._ddfprvsourcetime,
                          'PPDBAlert._prvforcedsourcetime': PPDBAlert._prvforcedsourcetime,
                          'PPDBAlert._ddfprvforcedsourcetime': PPDBAlert._ddfprvforcedsourcetime,
                          'PPDBAlert._sourcetime': PPDBAlert._sourcetime,
                         } )
        # self.logger.debug( 'Exiting' )


