from abc import ABC, abstractmethod
from gzip import decompress
import io
import logging
import requests

from astropy.io import fits
import healpy as hp
import numpy as np


logger = logging.getLogger(__name__)


class BaseParser(ABC):
    event = None

    def __init__(self, alert, *args, **kwargs):
        self.alert = alert

    @abstractmethod
    def parse(self):
        pass

    @staticmethod
    def get_confidence_regions(skymap_fits_url):
        try:
            buffer = io.BytesIO()
            buffer.write(decompress(requests.get(skymap_fits_url, stream=True).content))
            buffer.seek(0)
            hdul = fits.open(buffer, memmap=False)

            # Get the total number of healpixels in the map
            n_pixels = len(hdul[1].data)
            # Covert that to the nside parameter
            nside = hp.npix2nside(n_pixels)
            # Sort the probabilities so we can do the cumulative sum on them
            probabilities = hdul[1].data['PROB']
            probabilities.sort()
            # Reverse the list so that the largest pixels are first
            probabilities = probabilities[::-1]
            cumulative_probabilities = np.cumsum(probabilities)
            # The number of pixels in the 90 (or 50) percent range is just given by the first set of pixels that add up
            # to 0.9 (0.5)
            index_90 = np.min(np.flatnonzero(cumulative_probabilities >= 0.9))
            index_50 = np.min(np.flatnonzero(cumulative_probabilities >= 0.5))
            # Because the healpixel projection has equal area pixels, the total area is just the heal pixel area * the
            # number of heal pixels
            healpixel_area = hp.nside2pixarea(nside, degrees=True)
            area_50 = (index_50 + 1) * healpixel_area
            area_90 = (index_90 + 1) * healpixel_area

            return area_50, area_90
        except Exception as e:
            logger.error(f'Unable to parse {skymap_fits_url} for confidence regions: {e}')

        return None, None

    def is_alert_parsable(self):
        return False


class DefaultParser(BaseParser):

    def __repr__(self):
        return 'Default Parser'

    def parse(self):
        return {'message': self.alert}
