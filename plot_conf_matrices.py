import matplotlib.pyplot as plt
import pandas as pd
from sklearn.metrics import ConfusionMatrixDisplay


def plot_matrix(classifier_id, matrix):
    plt.figure(figsize=(15, 15))
    row = matrix.iloc[0]
    name = f'{row["broker_name"]} {row["classifier_name"]}'
    cmd = ConfusionMatrixDisplay.from_predictions(
        y_true=matrix['true_class'],
        y_pred=matrix['pred_class'],
        normalize='true',
        ax=plt.gca(),
    )
    cmd.ax_.get_images()[0].set_clim(0, 1)
    plt.title(name)
    plt.savefig(f'{name}.pdf')
    plt.close()


def main():
    matrices = pd.read_csv('conf_matrices.csv')
    for classifier_id, matrix in matrices.groupby('classifier_index'):
        plot_matrix(classifier_id, matrix)


if __name__ == '__main__':
    main()