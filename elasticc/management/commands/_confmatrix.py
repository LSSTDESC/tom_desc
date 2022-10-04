# This code is copied from and modified from sql_query_conf_matrices_objects.py
#  in elasticc_metrics

import argparse
import logging
import pathlib
import os
import psycopg2.extras
from pprint import pformat
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


class ConfMatrixClient:
    def __init__(self,  dbconn, logger=logging.getLogger(__name__) ):
        self.dbconn = dbconn
        self.logger = logger
        self.taxonomy = { -1: 'missed' }
        self.load_taxonomy()
        self.load_classifiers()
        
    def load_taxonomy(self):
        query = ( 'SELECT DISTINCT ON ("classId") "classId",description '
                  'FROM elasticc_gentypeofclassid GROUP BY "classId",description' )
        data = self.query( query )
        tmp = { i['classId']: i['description'] for i in data }
        # The nature of the taxonomy is such that we don't want to sort
        # it numerically.  More digits means a subclass, and we want
        # subclasses sorted after their class.  Effectively, this means
        # that we can right-zero pad everything out to the same number
        # of digits and then sort that numerically.
        maxdigits = max( [ len(str(i)) for i in tmp.keys() ] )
        tmp = dict( sorted( tmp.items(), key = lambda x: int( ( str(x[0]) + '0'*maxdigits )[0:maxdigits] ) ) )
        self.taxonomy.update( tmp )

    def load_classifiers(self):
        query = f'''
           SELECT * FROM elasticc_brokerclassifier
           ORDER BY "brokerName", "brokerVersion", "classifierName", "classifierId"
        '''
        data = self.query(query)
        self.logger.debug(pformat(data))
        self.classifiers = {row['classifierId']: f'{row["brokerName"]} {row["brokerVersion"]} {row["classifierName"]}'
                            for row in data}
        
    def query(self, query: str) -> List[Dict]:
        with self.dbconn.cursor( cursor_factory=psycopg2.extras.RealDictCursor ) as cursor:
            cursor.execute( query )
            return list( cursor.fetchall() )

    def get_classifications(self, *,
                            definition: str,
                            classifier_id: Optional[int],
                            include_missed: bool = False) -> Dict[str, pd.DataFrame]:
        if include_missed:
            best_last_join_type = 'LEFT'
            join_object_sent = '''
                INNER JOIN (
                    SELECT
                    "diaObjectId", bool_or("alertSentTimestamp" IS NOT NULL) AS "is_sent"
                        FROM elasticc_diaalert
                        GROUP BY "diaObjectId" 
                    ) object_sent_record
                        ON (elasticc_diaobjecttruth."diaObjectId" = object_sent_record."diaObjectId")
                    '''
            where = 'WHERE object_sent_record."is_sent"'
        else:
            best_last_join_type = 'INNER'
            join_object_sent = ''
            where = ''

        if definition == 'last_best':
            distinct_order = 'elasticc_diaalert."alertSentTimestamp", elasticc_brokerclassification."probability"'
        elif definition == 'best':
            # I think we need additional sorting over alertSentTimestamp to get the deterministic result for the
            # case for equal probabilities (I've seen prob of 1.0)
            distinct_order = 'elasticc_brokerclassification."probability", elasticc_diaalert."alertSentTimestamp"'
        else:
            raise ValueError(f'Unknown classification definition: {definition}')

        dfs = {}
        for classifier_id_, classifier_name in self.classifiers.items():
            if classifier_id is not None and classifier_id != classifier_id_:
                continue

            self.logger.info(f'Getting classifications for {classifier_name}')
            query = f'''
                SELECT best_last."classId" AS pred_class,
                       elasticc_gentypeofclassid."classId" AS true_class,
                       COUNT(*) AS n
                FROM elasticc_diaobjecttruth
                INNER JOIN elasticc_gentypeofclassid
                    ON (elasticc_diaobjecttruth.gentype = elasticc_gentypeofclassid.gentype)
                {join_object_sent}
                {best_last_join_type} JOIN
                (
                   SELECT DISTINCT ON (elasticc_diaalert."diaObjectId")
                      elasticc_brokerclassification."classId", elasticc_brokerclassification."probability",
                      elasticc_diaalert."diaObjectId"
                   FROM elasticc_brokerclassification
                   INNER JOIN elasticc_brokermessage
                      ON elasticc_brokerclassification."brokerMessageId"=elasticc_brokermessage."brokerMessageId"
                   INNER JOIN elasticc_diaalert
                      ON elasticc_brokermessage."alertId"=elasticc_diaalert."alertId"
                   WHERE elasticc_brokerclassification."classifierId"={classifier_id_}
                   ORDER BY elasticc_diaalert."diaObjectId", {distinct_order} DESC
                ) best_last
                ON (best_last."diaObjectId" = elasticc_diaobjecttruth."diaObjectId")
                {where}
                GROUP BY pred_class, true_class
                ORDER BY pred_class, true_class
            '''
            data = self.query(query)
            self.logger.debug(pformat(data))
            if len(data) == 0:
                self.logger.warning(f'No data for {classifier_name}')
                continue
            df = pd.DataFrame.from_records(data)
            df['classifier_id'] = classifier_id_
            df['classifier_name'] = classifier_name
            df['pred_class'] = df['pred_class'].fillna(-1).astype(int)
            dfs[classifier_id_] = df

        return dfs


    @np.vectorize
    def conf_annotation(count: int, fraction: float) -> str:
        percent = np.round(fraction * 100)
        if count < 1_000_000:
            count_str = str(count)
        else:
            count_str = f'{count:.3g}'
        return f'{percent}%\n{count_str}'

    def plot_matrix(self, matrix: pd.DataFrame, *, norm: str, namebyid:bool=False, extension:str="pdf",
                    plotdir:str=None ):
        import matplotlib.pyplot as plt
        import seaborn as sns
        from matplotlib.patches import Rectangle
        from sklearn.metrics import confusion_matrix

        plt.figure(figsize=(20, 20))
        plt.gca().set_aspect(
            # aspect=len(np.unique(matrix['true_class'])) / len(np.unique(matrix['pred_class'])),
            aspect='equal',
            adjustable='box',
        )
        name = matrix.iloc[0]['classifier_name']
        classifier_id = matrix.iloc[0]['classifier_id']
        counts = confusion_matrix(
            y_true=matrix['true_class'],
            y_pred=matrix['pred_class'],
            sample_weight=matrix['n'],
            normalize=None,
        )
        # Remove empty lines corresponding to missed and Other classes
        idx = np.where(np.sum(counts, axis=1) > 0)[0], np.where(np.sum(counts, axis=0) > 0)[0]
        counts = counts[idx[0], :][:, idx[1]]
        fractions = confusion_matrix(
            y_true=matrix['true_class'],
            y_pred=matrix['pred_class'],
            sample_weight=matrix['n'],
            normalize=norm,
        )[idx[0], :][:, idx[1]]
        annotations = self.conf_annotation(counts, fractions)
        true_labels = np.vectorize(self.taxonomy.get)(np.unique(matrix['true_class']))
        pred_labels = np.vectorize(self.taxonomy.get)(np.unique(matrix['pred_class']))
        sns.heatmap(fractions,
                    cmap='Blues', vmin=0, vmax=1,
                    annot=annotations, fmt='s', annot_kws={"fontsize": 10},
                    xticklabels=pred_labels, yticklabels=true_labels)
        for j, label in enumerate(true_labels):
            try:
                i = np.where(pred_labels == label)[0].item()
            except ValueError:
                self.logger.warning(f'{label} not found in predictions for {name}')
                continue
            plt.gca().add_patch(Rectangle((i, j), 1, 1, ec='black', fc='none', lw=2))
        plt.title(name)
        plt.xlabel('Predicted class')
        plt.ylabel('True class')
        plt.tight_layout()
        plotdir = pathlib.Path(".") if plotdir is None else pathlib.Path(plotdir)
        if namebyid:
            plt.savefig(plotdir / f'{classifier_id}.{extension}')
        else:
            plt.savefig(plotdir / f'{name}.{extension}')
        plt.close()
