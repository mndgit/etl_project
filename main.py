########################################################
# this file calls etl modules
########################################################

import extract
import transform
import load

#extract the data
extract.extractor()

#transform the data
transform.transformer()

#load the data to postgres and mongodb
load.loader()