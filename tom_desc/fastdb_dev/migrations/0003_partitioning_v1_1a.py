from django.db import migrations, models
from psqlextra.backend.migrations.operations import PostgresAddListPartition

class Migration(migrations.Migration):

    dependencies = [
        ('fastdb_dev', '0002_partitioning_v1_1'),
    ]

    operations = [
        PostgresAddListPartition(
           model_name="DStoPVtoSS",
           name='pv_v1_1a',
           values=['v1_1a',],
        ),
         PostgresAddListPartition(
           model_name="DFStoPVtoSS",
           name='pv_v1_1a',
           values=['v1_1a',],
        ),
     PostgresAddListPartition(
           model_name="DiaSource",
           name='pv_v1_1a',
           values=['v1_1a',],
        ),
      PostgresAddListPartition(
           model_name="DiaForcedSource",
           name='pv_v1_1a',
           values=['v1_1a',],
        ),
      ]
