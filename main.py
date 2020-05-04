########################################################
# this file calls etl modules
########################################################

import extract
import transform

rankings_df = extract.scrape()
rankings_country_code_population = transform.transform()