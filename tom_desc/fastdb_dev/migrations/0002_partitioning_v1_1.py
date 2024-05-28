from django.db import migrations, models
from psqlextra.backend.migrations.operations import PostgresAddListPartition

class Migration(migrations.Migration):

    dependencies = [
        ('fastdb_dev', '0001_initial'),
    ]

    operations = [
        PostgresAddListPartition(
           model_name="DStoPVtoSS",
           name='pv_v1_1',
           values=['v1_1',],
        ),
         PostgresAddListPartition(
           model_name="DFStoPVtoSS",
           name='pv_v1_1',
           values=['v1_1',],
        ),
     PostgresAddListPartition(
           model_name="DiaSource",
           name='pv_v1_1',
           values=['v1_1',],
        ),
      PostgresAddListPartition(
           model_name="DiaForcedSource",
           name='pv_v1_1',
           values=['v1_1',],
        ),
      ]
