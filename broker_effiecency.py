import pandas as pd


def efficiency(true_counts, matrix):
    row = matrix.iloc[0]
    name = f'{row["broker_name"]} {row["classifier_name"]}'
    for i in true_counts.index:
        try:
            pred_count = matrix.loc[(i, i)]['n']
        except KeyError:
            pred_count = 0
        print(name, i, pred_count / true_counts.loc[i]['n'])


def main():
    matrices = pd.read_csv('conf_matrices.csv', index_col=['true_class', 'pred_class'])
                         # usecols=['true_class', 'pred_class', 'n', 'classifier'], dtype=int)
    true_counts = pd.read_csv('classes.csv', index_col='true_class', dtype=int)
    for _, matrix in matrices.groupby('classifier_index'):
        efficiency(true_counts, matrix)


if __name__ == '__main__':
    main()