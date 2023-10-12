import sys
import os
import io
import socket
import math
import pathlib
import logging
import json
import time
import datetime
import multiprocessing
import signal
import queue
import confluent_kafka
import fastavro

from django.core.management.base import BaseCommand, CommandError
from django_extensions.management.signals import post_command
from django_extensions.management.utils import signalcommand
import django.db
from django.db import transaction
from elasticc2.models import PPDBAlert

_rundir = pathlib.Path(__file__).parent

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
        overall_t0 = time.perf_counter()
        while not done:
            try:
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
                          'PPDBAlert._firstsourcetime': PPDBAlert._firstsourcetime,
                          'PPDBAlert._ddffirstsourcetime': PPDBAlert._ddffirstsourcetime,
                          'PPDBAlert._prvsourcetime': PPDBAlert._prvsourcetime,
                          'PPDBAlert._ddfprvsourcetime': PPDBAlert._ddfprvsourcetime,
                          'PPDBAlert._prvforcedsourcetime': PPDBAlert._prvforcedsourcetime,
                          'PPDBAlert._ddfprvforcedsourcetime': PPDBAlert._ddfprvforcedsourcetime,
                          'PPDBAlert._sourcetime': PPDBAlert._sourcetime,
                         } )
        # self.logger.debug( 'Exiting' )


class Command(BaseCommand):
    help = 'Send ELAsTiCC2 Alerts'

    # This is kind of ugly.  But, what I really want to do is have
    # a post-command callback that gets called on the command instance.
    # Capturing signals doesn't work inside django management commands,
    # and the django post_command thingy has the *class*, not the
    # object, as the argument.
    _instance = None

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )
        self.__class__._instance = self
        self.logger = logging.getLogger( "send_elasticc_alerts" )
        self.logger.propagate = False
        if not self.logger.hasHandlers():
            logout = logging.StreamHandler( sys.stderr )
            self.logger.addHandler( logout )
            formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                           datefmt='%Y-%m-%d %H:%M:%S' )
            logout.setFormatter( formatter )
        self.logger.setLevel( logging.INFO )

    def add_arguments( self, parser ):
        parser.add_argument( '--start-day', type=float, default=None,
                             help=( "Sets simulation date: stream alerts starting with this midPointTai. "
                                    "Will ignore all alerts from before this date.  Will start at either "
                                    "this date, or, if -a is given, the latest alert that has a non-NULL "
                                    "alertsenttimestamp (if that is greater)" ) )
        parser.add_argument( '-d', '--through-day', type=float, default=None,
                             help=( "Sets simulation date: stream alerts with source through this midPointTai. "
                                    "Must give one of this or -a." ) )
        parser.add_argument( '-a', '--added-days', type=float, default=None,
                             help=( "Will look at greatest midpoitntai on alerts sent, and will then go to that day "
                                    "plus this many days, rounded down to the last 0.5.  (0.5 because 12:00 UTC "
                                    "is 8:00 Cero Pachon time, which should be after a night's worth of "
                                    "observations.)  Ignored if --through-day is given." ) )
        parser.add_argument( '-k', '--kafka-server', default='brahms.lbl.gov:9092', help="Kafka server to stream to" )
        parser.add_argument( '--wfd-topic', default='alerts-wfd', help="Topic to stream WFD alerts to" )
        parser.add_argument( '--ddf-full-topic', default='alerts-ddf-full', help="Topic to stream full DDF alerts to" )
        parser.add_argument( '--ddf-limited-topic', default='alerts-ddf-limited',
                             help="Topic to stream limited DDF alerts to" )
        parser.add_argument( '-s', '--alert-schema', default=f'{_rundir}/elasticc.v0_9_1.alert.avsc',
                             help='File with AVRO schema' )
        parser.add_argument( '-f', '--flush-every', default=1000, type=int,
                             help="Flush the kafka producer every this man alerts" )
        parser.add_argument( '-l', '--log-every', default=10000, type=int,
                             help="Log alerts sent at this interval; 0=don't log" )
        parser.add_argument( '-r', '--runningfile', default=f'{_rundir}/isrunning.log',
                             help=( "Will write to this file when run starts, delete when done.  Will not start "
                                    "if this file exists." ) )
        parser.add_argument( '-n', '--num-reconstruct-processes', default=3, type=int,
                             help="Run this many alert reconstruction subprocesses (default 3)" )
        parser.add_argument( '--do', action='store_true', default=False,
                             help="Actually do it (otherwise, it's a dry run)" )

    @transaction.atomic
    def update_alertsent( self, ids ):
        sentalerts = PPDBAlert.objects.filter( pk__in=ids )
        for sa in sentalerts:
            sa.alertsenttimestamp = datetime.datetime.now( datetime.timezone.utc )
            sa.save()

    def interruptor( self, signum, frame ):
        self.logger.error( "Got an interrupt signal, cleaning up and existing." )
        self.cleanup()

    def cleanup( self ):
        self.logger.info( "In cleanup" )
        for subproc in multiprocessing.active_children():
            subproc.kill()
        for key, val in self.procinfo.items():
            val['proc'].terminate()
            val['proc'].close()
        self.runningfile.unlink()

    @signalcommand
    def handle( self, *args, **options ):

        # There is a race condition built-in here -- if the file is created by
        #   another process between when I check if it exists and when I create
        #   it here, then both processes will merrily run.  Since my use case
        #   is a nightly cron job, and I want to make sure that the previous
        #   night has finished before I start the next one, this shouldn't
        #   be a practical problem.
        self.runningfile = pathlib.Path( options['runningfile'] )
        if self.runningfile.exists():
            self.logger.warn( f"{self.runningfile} exists, not starting." )
            return

        # ...this doesn't seem to work inside a django management command.
        # The signals are never caught.
        # I hate that.  I wish there was a way to override it.
        # signal.signal( signal.SIGINT, lambda signum, frame: self.interruptor( signum, frame ) )
        # signal.signal( signal.SIGTERM, lambda signum, frame: self.interruptor( signum, frame ) )

        self.procinfo = {}
        try:
            with open( self.runningfile, "w" ) as ofp:
                ofp.write( f"{datetime.datetime.now().isoformat()} on host {socket.gethostname()}\n" )

            self.logger.info( "Figuring out starting day" )

            if ( ( ( options['through_day'] is None ) == ( options['added_days'] is None ) )
                 or
                 ( ( options['through_day'] is None ) and ( options['added_days'] is None ) ) ):
                raise RuntimeError( f"Must give exactly one of -d or -a: "
                                    f"-d was {options['through_day']} (type {type(options['through_day'])}) "
                                    f"and -a was {options['added_days']} (type {type(options['added_days'])}) " )

            # import pdb; pdb.set_trace()
            start_t = options['start_day']
            if options['through_day'] is not None:
                through_day = options['through_day']
            else:
                lastalertquery = ( PPDBAlert.objects
                                   .filter( alertsenttimestamp__isnull=False )
                                   .order_by( '-diasource__midpointtai' ) )
                try:
                    lastalert = lastalertquery[0]
                    infostr = f"Last alert sent had midpointtai {lastalert.diasource.midpointtai}"
                    if ( start_t is None) or ( lastalert.diasource.midpointtai > start_t ):
                        start_t = lastalert.diasource.midpointtai
                    else:
                        infostr += f" but start_day {start_t} is bigger"
                    self.logger.info( infostr )
                except IndexError as ex:
                    # No alerts have been sent yet, so find the first one
                    if options['start_day'] is not None:
                        self.logger.info( f"No alerts have been sent yet, starting with mjd {options['start_day']}" )
                        start_t = options['start_day']
                    else:
                        self.logger.info( "No alerts have been sent yet, figuring out the time of the first one." )
                        firstalertquery = PPDBAlert.objects.order_by( 'diasource__midpointtai' )
                        start_t = firstalertquery[0].diasource.midpointtai - 1
                        self.logger.info( f"First alert is at MJD {start_t+1}" )
                through_day = math.floor( start_t + 0.5 ) + options['added_days'] + 0.5

            self.logger.info( f"Sending alerts query for unsent alerts"
                              f"{f' from {start_t}' if start_t is not None else ''} through {through_day}" )

            alerts = ( PPDBAlert.objects
                       .filter( alertsenttimestamp__isnull=True, diasource__midpointtai__lte=through_day ) )
            if start_t is not None:
                alerts = alerts.filter( diasource__midpointtai__gte=start_t )
            alerts = alerts.order_by( 'diasource__midpointtai' )
            self.logger.info( f"{alerts.count()} alerts to stream" )

            if alerts.count() == 0:
                self.logger.info( "No alerts found, exiting." )
                return
            alerts = alerts.all()

            self.logger.info( "**** streaming starting ****" )
            self.logger.info( f"Streaming to {options['kafka_server']} topics "
                              f"{options['wfd_topic']}, {options['ddf_full_topic']}, {options['ddf_limited_topic']}" )
            self.logger.info( f"Streaming alerts through midPointTai {through_day}" )

            if options['do']:
                producer = confluent_kafka.Producer( { 'bootstrap.servers': options[ 'kafka_server' ],
                                                       'batch.size': 131072,
                                                       'linger.ms': 50 } )

            totflushed = 0
            nextlog = 0
            nddf = 0
            _tottime = 0
            _commtime = 0
            _flushtime = 0
            _updatealertsenttime = 0
            _producetime = 0
            overall_t0 = time.perf_counter()

            # Need to make sure that each subprocesses gets its own
            # database connection.  To that end, close the django
            # connections so that there aren't any cached ones.
            django.db.connections.close_all()

            def launchReconstructor( pipe ):
                reconstructor = AlertReconstructor( self, pipe, options['alert_schema'] )
                reconstructor.go()

            freeprocs = set()
            busyprocs = set()
            donealerts = set()
            ids_produced = []
            self.logger.info( f'Launching {options["num_reconstruct_processes"]} alert reconstruction subprocesses.' )
            for i in range( options['num_reconstruct_processes'] ):
                parentconn, childconn = multiprocessing.Pipe()
                proc = multiprocessing.Process( target=lambda: launchReconstructor( childconn ) )
                proc.start()
                self.procinfo[ proc.pid ] = { 'proc': proc,
                                              'parentconn': parentconn,
                                              'childconn': childconn }
                freeprocs.add( proc.pid )

            alertdex = 0
            nextlog = 0
            while ( alertdex < len(alerts) ) or ( len(busyprocs) > 0 ):
                # if ( alertdex >= len(alerts) ):
                #     self.logger.debug( "Waiting on {len(busyprocs)} busy processes" )
                #     time.sleep(1)
                if ( options['log_every'] > 0 ) and ( alertdex >= nextlog ):
                    self.logger.info( f"Have started {alertdex} of {len(alerts)} alerts, {totflushed} flushed." )
                    self.logger.info( f"    Timings: overall {time.perf_counter() - overall_t0}\n"
                                      f"                    _commtime : {_commtime}\n"
                                      f"                   _flushtime : {_flushtime}\n"
                                      f"                 _producetime : {_producetime}\n"
                                      f"         _updatealertsenttime : {_updatealertsenttime}\n" )
                    nextlog += options['log_every']

                # Submit alerts to any free processes
                t0 = time.perf_counter()
                # self.logger.debug( f"Sending work to {len(freeprocs)} free processes." )
                while ( alertdex < len(alerts) ) and ( len(freeprocs) > 0 ):
                    pid = freeprocs.pop()
                    busyprocs.add( pid )
                    # self.logger.debug( f"Sending alert dex {alertdex}, id {alerts[alertdex].alert_id} "
                    #                    f"to process {pid}" )
                    self.procinfo[pid]['parentconn'].send( { 'command': 'do',
                                                             'alertdex': alertdex,
                                                             'alert': alerts[alertdex] } )
                    if alerts[alertdex].diaobject.isddf:
                        nddf += 1
                    alertdex += 1
                _commtime += time.perf_counter() - t0

                # Check for response from busy processes
                # self.logger.debug( f'Checking {len(busyprocs)} busy processes for responses.' )
                doneprocs = set()
                for pid in busyprocs:
                    t0 = time.perf_counter()
                    if not self.procinfo[pid]['parentconn'].poll():
                        continue
                    doneprocs.add( pid )

                    msg = self.procinfo[pid]['parentconn'].recv()
                    if ( 'response' not in msg ) or ( msg['response'] != 'alert produced' ):
                        raise ValueError( f"Unexpected response from child process: {msg}" )
                    # self.logger.debug( f"Got response from {pid} for alertdex {msg['alertdex']}" )

                    alertid = msg['alertid']
                    respalertdex = msg['alertdex']
                    if alertid in donealerts:
                        raise RuntimeError( f'{msg["alertid"]} got processed more than once' )
                    donealerts.add( alertid )
                    _commtime += time.perf_counter() - t0

                    if options['do']:
                        t0 = time.perf_counter()
                        if alerts[respalertdex].diaobject.isddf:
                            producer.produce( options['ddf_full_topic'], msg['fullhistory'] )
                            producer.produce( options['ddf_limited_topic'], msg['limitedhistory'] )
                        else:
                            producer.produce( options['wfd_topic'], msg['fullhistory'] )
                        ids_produced.append( alertid )
                        _producetime += time.perf_counter() - t0

                    if len(ids_produced) >= options['flush_every']:
                        if options['do']:
                            t0 = time.perf_counter()
                            producer.flush()
                            totflushed += len( ids_produced )
                            t1 = time.perf_counter()
                            self.update_alertsent( ids_produced )
                            t2 = time.perf_counter()
                            _flushtime += t1 - t0
                            _updatealertsenttime += t2 - t1
                        ids_produced = []

                # self.logger.debug( f"{len(doneprocs)} finished, adding them back to freeprocs" )
                # ****
                # if len(doneprocs) == 0:
                #     time.sleep(1)
                # ****
                for pid in doneprocs:
                    busyprocs.remove( pid )
                    freeprocs.add( pid )

            if len(ids_produced) > 0:
                if options['do']:
                    t0 = time.perf_counter()
                    producer.flush()
                    totflushed += len( ids_produced )
                    t1 = time.perf_counter()
                    self.update_alertsent( ids_produced )
                    t2 = time.perf_counter()
                    _flushtime += t1 - t0
                    _updatealertsenttime += t2 - t1
                ids_produced = []

            # Tell all subprocesses to end
            subtimings = {}
            for pid, proc in self.procinfo.items():
                proc['parentconn'].send( { 'command': 'die' } )
                msg = proc['parentconn'].recv()
                for key, val in msg.items():
                    if key != 'response':
                        if key not in subtimings:
                            subtimings[key] = val
                        else:
                            subtimings[key] += val

            _tottime += time.perf_counter() - overall_t0

            self.logger.info( f"**** Done sending {len(alerts)} alerts (incl. {nddf} DDF); {totflushed} flushed ****" )
            strio = io.StringIO()
            strio.write( f"Timings: overall {_tottime}\n"
                         f"                _commtime : {_commtime}\n"
                         f"               _flushtime : {_flushtime}\n"
                         f"             _producetime : {_producetime}\n"
                         f"     _updatealertsenttime : {_updatealertsenttime}\n"
                         f"                      Sum over subprocesses:\n"
                         f"                      ----------------------\n" )
            for key,  val in subtimings.items():
                strio.write( f"{key:>36s} : {val}\n" )
            self.logger.info( strio.getvalue() )

        finally:
            self.logger.info( "I really hope cleanup gets called." )
            # self.cleanup()

def post_command_handler( sender, **kwargs ):
    sys.stderr.write( f"In post_command_handler; sender is a {type(sender)}, "
                      f"Command._instance is a {type(Command._instance)}\n" )
    Command._instance.cleanup()

post_command.connect( post_command_handler, Command )
