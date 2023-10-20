import sys
import re
import pathlib
import datetime
import logging
import psycopg2
import psycopg2.extras
import django.db
from matplotlib import pyplot
from django.core.management.base import BaseCommand, CommandError

_rundir = pathlib.Path(__file__).parent

_logger = logging.getLogger( __name__ )
_logger.propagate = False
_logout = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logout )
_formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S' )
_logout.setFormatter( _formatter )
_logger.setLevel( logging.INFO )

class Command(BaseCommand):
    help = 'Generate alert stream rate histograms'
    outdir = _rundir / "../../static/elasticc2/alertstreamhists"

    def add_arguments( self, parser) :
        parser.add_argument( '--start', default='2023-07-05',
                             help='YYYY-MM-DD of first day to look at (default: 2023-07-05)' )
        parser.add_argument( '--end', default=None,
                             help='YYYY-MM-DD of last day to look at (default: current day - 1)' )
        parser.add_argument( '--hour', default=0,
                             help='UTC Hour where the "day" starts (default: 0)' )

    def handle( self, *args, **options ):
        self.outdir.mkdir( parents=True, exist_ok=True )

        starthour = int( options['hour'] )
        datematch = re.compile( '^(\d{4})-(\d{2})-(\d{2})$' )
        match = datematch.search( options['start'] )
        if match is None:
            _logger.error( f"Failed to parse start date {options['start']} for yyyy-mm-dd" )
            return
        start = datetime.date( int(match.group(1)), int(match.group(2)), int(match.group(3)) )
        if options['end'] is not None:
            match = datematch.search( options['end'] )
            if match is None:
                _logger.error( f"Failed to parse start date {options['end']} for yyyy-mm-dd" )
                return
            end = datetime.date( int(match.group(1)), int(match.group(2)), int(match.group(3)) )
        else:
            enddt = datetime.datetime.now() - datetime.timedelta( days=1 )
            end = datetime.date( enddt.year, enddt.month, enddt.day )
        
        # Jump through hoops to get access to the psycopg2 connection from django
        conn = django.db.connection.cursor().connection

        with conn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
            curdate = start
            while curdate <= end:
                fpath = self.outdir / f"{curdate.year:04d}-{curdate.month:02d}-{curdate.day:02d}.svg"
                if not fpath.exists():
                    _logger.info( f"Generating {fpath.name}" )

                    searchtime = datetime.datetime( curdate.year, curdate.month, curdate.day, starthour )
                    endsearchtime = searchtime + datetime.timedelta( days=1 )
                    now = searchtime
                    hour = []
                    xticklabel = []
                    num = []
                    while now < endsearchtime:
                        cursor.execute( 'SELECT COUNT(*) FROM elasticc2_ppdbalert '
                                        'WHERE "alertsenttimestamp">=%(start)s '
                                        '  AND "alertsenttimestamp"<%(oneh)s',
                                        { 'start': now.isoformat(),
                                          'oneh': (now + datetime.timedelta(hours=1)).isoformat() } )
                        row = cursor.fetchone()
                        hour.append( now.hour if now.hour >= starthour else now.hour + 24 )
                        xticklabel.append( now.hour )
                        num.append( row['count'] )
                        now += datetime.timedelta( hours=1 )

                    fig = pyplot.figure( figsize=(12,4), tight_layout=True )
                    ax = fig.add_subplot( 1, 1, 1 )
                    ax.set_title( f'{searchtime.year}-{searchtime.month}-{searchtime.day} to '
                                  f'{endsearchtime.year}-{endsearchtime.month}-{endsearchtime.day} '
                                  f'({sum(num):,} alerts)',
                                  fontsize=16 )
                    ax.set_xlabel( 'UTC hour', fontsize=14 )
                    ax.set_ylabel( 'N alerts streamed', fontsize=14 )
                    ax.bar( hour, num, align='edge', width=1.0 )
                    ax.set_xticks( hour, xticklabel )
                    ax.tick_params( 'both', labelsize=12 )
                    fig.savefig( fpath )
                    pyplot.close( fig )
                    
                curdate += datetime.timedelta( days=1 )
