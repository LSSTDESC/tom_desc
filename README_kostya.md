# Completeness confusion matrix for alerts

```sh
python3 sql_query_conf_matrices_alerts.py
python3 plot_conf_matrices.py
```

# Completeness confusion matrix for objects

It overwrites previous files

```sh
python3 sql_query_conf_matrices_objects.py
python3 plot_conf_matrices.py
```

# Classification efficiency

Number of true positives returned as a last classification by a broker to a total number of objects of this class

```sh
python3 sql_query_conf_matrices_objects.py
python3 broker_effiecency.py
```