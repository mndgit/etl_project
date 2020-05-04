########################################################
# this file calls etl modules
########################################################

import extract
import transform

#extract the data
extract.scrape()

#transform the data
transform.transform()

#load the data to postgres and mongodb