import pandas as pd
from googletrans import Translator
import time
import sys

#pytrends library from https://github.com/GeneralMills/pytrends
from pytrends.request import TrendReq
pytrends = TrendReq(hl='en-US', tz=360)

#credentials for the database
import psycopg2
import psql_creds
from sqlalchemy import create_engine

#country and languages for all countries
country_list = [['AF', 'fa'], ['AX', 'sv'], ['AL', 'sq'], ['DZ', 'ar'], ['AS', 'en'], ['AD', 'ca'], ['AO', 'pt'], ['AI', 'en'], ['AQ', 'en'], ['AG', 'en'], ['AR', 'es'], ['AM', 'en'], ['AW', 'nl'], ['AU', 'en'], ['AT', 'de'], ['AZ', 'az'], ['BS', 'en'], ['BH', 'ar'], ['BD', 'bn'], ['BB', 'en'], ['BY', 'ru'], ['BE', 'nl'], ['BZ', 'en'], ['BJ', 'fr'], ['BM', 'en'], ['BT', 'en'], ['BO', 'es'], ['BQ', 'nl'], ['BA', 'en'], ['BW', 'en'], ['BV', 'no'], ['BR', 'pt'], ['IO', 'en'], ['VG', 'en'], ['BN', 'ms'], ['BG', 'bg'], ['BF', 'fr'], ['BI', 'fr'], ['KH', 'en'], ['CM', 'fr'], ['CA', 'en'], ['CV', 'pt'], ['KY', 'en'], ['CF', 'fr'], ['TD', 'fr'], ['CL', 'es'], ['CN', 'zh-CN'], ['CX', 'en'], ['CC', 'en'], ['CO', 'es'], ['KM', 'ar'], ['CK', 'en'], ['CR', 'es'], ['HR', 'hr'], ['CU', 'es'], ['CW', 'nl'], ['CY', 'en'], ['CZ', 'cs'], ['CD', 'fr'], ['DK', 'da'], ['DJ', 'fr'], ['DM', 'en'], ['DO', 'es'], ['TL', 'pt'], ['EC', 'es'], ['EG', 'ar'], ['SV', 'es'], ['GQ', 'es'], ['ER', 'ar'], ['EE', 'et'], ['ET', 'en'], ['FK', 'en'], ['FO', 'da'], ['FJ', 'en'], ['FI', 'fi'], ['FR', 'fr'], ['GF', 'fr'], ['PF', 'fr'], ['TF', 'fr'], ['GA', 'fr'], ['GM', 'en'], ['GE', 'ka'], ['DE', 'de'], ['GH', 'en'], ['GI', 'en'], ['GR', 'el'], ['GL', 'da'], ['GD', 'en'], ['GP', 'fr'], ['GU', 'en'], ['GT', 'es'], ['GG', 'en'], ['GN', 'fr'], ['GW', 'pt'], ['GY', 'en'], ['HT', 'fr'], ['HM', 'en'], ['HN', 'es'], ['HK', 'zh-TW'], ['HU', 'hu'], ['IS', 'is'], ['IN', 'en'], ['ID', 'id'], ['IR', 'fa'], ['IQ', 'ar'], ['IE', 'en'], ['IM', 'en'], ['IL', 'en'], ['IT', 'it'], ['CI', 'fr'], ['JM', 'en'], ['JP', 'ja'], ['JE', 'en'], ['JO', 'ar'], ['KZ', 'ru'], ['KE', 'en'], ['KI', 'en'], ['XK', 'sr'], ['KW', 'ar'], ['KG', 'ru'], ['LA', 'en'], ['LV', 'lv'], ['LB', 'en'], ['LS', 'en'], ['LR', 'en'], ['LY', 'ar'], ['LI', 'de'], ['LT', 'lt'], ['LU', 'fr'], ['MO', 'zh-CN'], ['MK', 'mk'], ['MG', 'fr'], ['MW', 'en'], ['MY', 'ms'], ['MV', 'en'], ['ML', 'fr'], ['MT', 'en'], ['MH', 'en'], ['MQ', 'fr'], ['MR', 'fr'], ['MU', 'fr'], ['YT', 'fr'], ['MX', 'es'], ['FM', 'en'], ['MD', 'ro'], ['MC', 'fr'], ['MN', 'en'], ['ME', 'sr'], ['MS', 'en'], ['MA', 'fr'], ['MZ', 'pt'], ['MM', 'en'], ['NA', 'en'], ['NR', 'en'], ['NP', 'en'], ['NL', 'nl'], ['AN', 'nl'], ['NC', 'fr'], ['NZ', 'en'], ['NI', 'es'], ['NE', 'fr'], ['NG', 'en'], ['NU', 'en'], ['NF', 'en'], ['KP', 'ko'], ['MP', 'en'], ['NO', 'no'], ['OM', 'ar'], ['PK', 'en'], ['PW', 'en'], ['PS', 'ar'], ['PA', 'es'], ['PG', 'en'], ['PY', 'es'], ['PE', 'es'], ['PH', 'en'], ['PN', 'en'], ['PL', 'pl'], ['PT', 'pt'], ['PR', 'es'], ['QA', 'ar'], ['CG', 'fr'], ['RE', 'fr'], ['RO', 'ro'], ['RU', 'ru'], ['RW', 'fr'], ['BL', 'fr'], ['SH', 'en'], ['KN', 'en'], ['LC', 'en'], ['MF', 'fr'], ['PM', 'fr'], ['VC', 'en'], ['WS', 'en'], ['SM', 'it'], ['ST', 'pt'], ['SA', 'ar'], ['SN', 'fr'], ['RS', 'sr'], ['CS', 'sr'], ['SC', 'fr'], ['SL', 'en'], ['SG', 'en'], ['SX', 'nl'], ['SK', 'sk'], ['SI', 'sl'], ['SB', 'en'], ['SO', 'en'], ['ZA', 'en'], ['GS', 'en'], ['KR', 'ko'], ['SS', 'en'], ['ES', 'es'], ['LK', 'en'], ['SD', 'ar'], ['SR', 'nl'], ['SJ', 'no'], ['SZ', 'en'], ['SE', 'sv'], ['CH', 'de'], ['SY', 'ar'], ['TW', 'zh-TW'], ['TJ', 'ru'], ['TZ', 'en'], ['TH', 'th'], ['TG', 'fr'], ['TK', 'en'], ['TO', 'en'], ['TT', 'en'], ['TN', 'ar'], ['TR', 'tr'], ['TM', 'en'], ['TC', 'en'], ['TV', 'en'], ['VI', 'en'], ['UG', 'en'], ['UA', 'uk'], ['AE', 'ar'], ['GB', 'en'], ['US', 'en'], ['UM', 'en'], ['UY', 'es'], ['UZ', 'ru'], ['VU', 'en'], ['VA', 'it'], ['VE', 'es'], ['VN', 'vi'], ['WF', 'fr'], ['EH', 'ar'], ['YE', 'ar'], ['ZM', 'en'], ['ZW', 'en']]

#list of words to update
word_list = list(pd.read_csv('words.csv', usecols=range(0,1)).iloc[:,0])

#initializing master dataframe
master = pd.DataFrame(columns=['id', 'date', 'country', 'trend_value', 'en_word', 'word'])

#first loop iterating on countries
for country in country_list:
    print(country[0])
    country_name = country[0]
    language = country[1]
#second loop iterating on words
    for word in word_list:
        translator = Translator()
        
        #try retry for google translate
        tries = 10
        for i in range(tries):
            try:
                translation = translator.translate(word, src='en', dest=language).text
            except KeyError as e:
                if i < tries - 1: # i is zero indexed
                    continue
                else:
                    pass
            break
            
        kw_list = [translation]
        try:
            #pytrends api call
            pytrends.build_payload(kw_list, cat=0, timeframe='all', geo=country_name, gprop='')     
            tmp = pytrends.interest_over_time()
            #sleep to avoid rate limits
            time.sleep(2)
            
            #set date to index
            tmp = tmp.reset_index().drop(['isPartial'],axis=1)
            tmp.columns = ['date', 'trend_value']
            tmp['en_word'] = word
            tmp['word'] = translation
            tmp['country'] = country_name
            tmp['id'] = tmp['date'].dt.strftime('%Y-%m-%d') + '|' + tmp['country'] + '|' + tmp['en_word']
            tmp = tmp.loc[:,['id', 'date', 'country', 'trend_value', 'en_word', 'word']]
            master = master.append(tmp, ignore_index=True)
        except:
            pass

#connecting to the postgres database, argv 1 is password
conn_string = "host="+ psql_creds.PGHOST +" port="+ "5432" +" dbname="+ psql_creds.PGDATABASE +" user=" + psql_creds.PGUSER +" password="+ sys.argv[1]
conn = psycopg2.connect(conn_string)

print("Updating database")
engine = create_engine('postgresql://' + psql_creds.PGUSER + ':' + sys.argv[1] + '@' + psql_creds.PGHOST + ':5432/' + psql_creds.PGDATABASE)
master.to_sql('google_trends', engine, if_exists = 'replace', index=False)
