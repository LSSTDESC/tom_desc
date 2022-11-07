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
    help = 'Generate broker stream rate graphs'
    outdir = _rundir / "../../static/elasticc/brokerstreamgraphs"

    def add_arguments( self, parser) :
        parser.add_argument( '--start', default='2022-09-28',
                             help='YYYY-MM-DD of first day to look at (default: 2022-09-28)' )
        parser.add_argument( '--end', default=None,
                             help='YYYY-MM-DD of last day to look at (default: current day - 1)' )
        parser.add_argument( '--hour', type=int, default=18,
                             help='UTC Hour where the "day" starts (default: 18)' )
        parser.add_argument( '--hourquantum', type=int, default=4,
                             help="How many hours per bin (default: 4)" )

    def handle( self, *args, **options ):
        self.outdir.mkdir( parents=True, exist_ok=True )

        starthour = int( options['hour'] )
        datematch = re.compile( '^(\d{4})-(\d{2})-(\d{2})$' )
        match = datematch.search( options['start'] )
        dhours = options['hourquantum']
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
            cursor.execute( 'SELECT * FROM elasticc_brokerclassifier '
                            'ORDER BY "brokerName","brokerVersion","classifierName","classifierParams"' )
            rows = cursor.fetchall()
            brokergroups = {}
            for row in rows:
                if row['brokerName'] not in brokergroups:
                    brokergroups[row['brokerName']] = []
                brokergroups[row['brokerName']].append( row['classifierId'] )

            for broker in brokergroups.keys():
                curdate = start
                while curdate <= end:
                    # _logger.info( f"Doing {broker} {curdate.strftime('%y-%m-%d')}" )
                    fpath = self.outdir / f"{broker}_{curdate.year:04d}-{curdate.month:02d}-{curdate.day:02d}.svg"
                    if fpath.exists():
                        _logger.info( f"{fpath.name} already exists, not regenerating" )
                    else:
                        _logger.info( f"Generating {fpath.name}" )

                        searchtime = datetime.datetime( curdate.year, curdate.month, curdate.day, starthour )
                        endsearchtime = searchtime + datetime.timedelta( days=1 )
                        now = searchtime
                        hour = []
                        xticklabel = []
                        num = []
                        while now < endsearchtime:
                            _logger.info( f"...{now.strftime('%H:%M')} to "
                                          f"{(now+datetime.timedelta(hours=dhours)).strftime('%H:%M')}" )
                            cursor.execute( 'SELECT COUNT("brokerMessageId") FROM '
                                            '( SELECT DISTINCT m."brokerMessageId" FROM elasticc_brokermessage m '
                                            '  INNER JOIN elasticc_brokerclassification c '
                                            '    ON m."brokerMessageId"=c."brokerMessageId" '
                                            '  WHERE c."classifierId" IN %(cfers)s '
                                            '     AND m."descIngestTimestamp">=%(start)s '
                                            '     AND m."descIngestTimestamp"<%(end)s '
                                            '  ) subq',
                                            { 'start': now.isoformat(),
                                              'end': (now + datetime.timedelta(hours=dhours)).isoformat(),
                                              'cfers': tuple( brokergroups[broker] ) } )
                            row = cursor.fetchone()
                            hour.append( now.hour if now.hour >= starthour else now.hour + 24 )
                            xticklabel.append( now.hour )
                            num.append( row['count'] )
                            now += datetime.timedelta( hours=dhours )

                        fig = pyplot.figure( figsize=(12,4), tight_layout=True )
                        ax = fig.add_subplot( 1, 1, 1 )
                        ax.set_title( f'{broker} {searchtime.year}-{searchtime.month}-{searchtime.day} to '
                                      f'{endsearchtime.year}-{endsearchtime.month}-{endsearchtime.day} '
                                      f'({sum(num):,} messages)',
                                      fontsize=16 )
                        ax.set_xlabel( 'UTC hour', fontsize=14 )
                        ax.set_ylabel( 'N messages received', fontsize=14 )
                        ax.bar( hour, num, align='edge', width=dhours )
                        ax.set_xticks( hour, xticklabel )
                        ax.tick_params( 'both', labelsize=12 )
                        fig.savefig( fpath )
                        pyplot.close( fig )

                    curdate += datetime.timedelta( days=1 )
