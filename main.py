########################################################
# this file calls etl modules
########################################################

import extract
import transform
import load

#extract the data
extract.main()

#transform the data
transform.main()

#load the data to postgres and mongodb
load.main()