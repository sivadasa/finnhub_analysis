import finnhub
import os
import json
import time
import csv
from os.path import exists
from shutil import copyfile

def write_file(data, filename):
    f=open(filename, 'a+')
    f.write(data)
    f.close()


def compare_sp_russell(sp_1500,russell_3000):
    common=[]
    russell_remaining=[]
    for ticker in russell_3000:
        if ticker in sp_1500:
            common.append(ticker)
        else:
            russell_remaining.append(ticker)
    return common, russell_remaining

def copy_file(common, sp_output, russell_output):

    downloaded_ticker=[]
    for root, dirs, files in os.walk(sp_output):
        for f in files:
            downloaded_ticker.append([f.split("_")[0], f])

    for ticker in common:
        for d_ticker in downloaded_ticker:
            if ticker==d_ticker[0]:
                src=os.path.join(sp_output,d_ticker[1])
                dest=os.path.join(russell_output,d_ticker[1])

                copyfile(src,dest)
                print("copying file :" + d_ticker[1])




def readcsv(filename):
    input_file=open(filename)
    tickers=input_file.read()
    #csv.reader(csv_file, delimiter=',')
    ticker_list=tickers.replace("'",'').split(",")
    ticker_list=list(map(lambda x:x.strip(), ticker_list))
    input_file.close()
    return ticker_list


def getcsv(russell_ticker, sp_ticker, russell_output, sp_output ):
    input_file=open(sp_ticker)
    tickers=input_file.read()
    #csv.reader(csv_file, delimiter=',')
    sp_1500=tickers.replace("'",'').replace("\n", '').split(",")
    #remove whitespace
    sp_1500=list(map(lambda x:x.strip(), sp_1500))
    input_file.close()

    input_file=open(russell_ticker)
    tickers=input_file.read()
    #csv.reader(csv_file, delimiter=',')
    russell_3000=tickers.replace("'",'').replace("\n", '').split(",")
    input_file.close()

    common, russell_remaining =compare_sp_russell(sp_1500,russell_3000)
    copy_file(common, sp_output, russell_output)

    return russell_remaining

def filtered_result(result_transcript_list, processed_filename):
    filter_year_list=[2019, 2020, 2021]
    result_ids=[]
    if exists(processed_filename):
        processed_ids=readcsv(processed_filename)
    else:
        processed_ids=[]
    
    for item in result_transcript_list:
       if (item['year'] in filter_year_list) and (item['id'] not in processed_ids):
           result_ids.append(item['id'])
    return result_ids

def process_transcripts(ticker_list, finnhub_client, output_text_dir, processed_filename):
    
    max_tries=5
    count_downloaded=0
    for ticker in ticker_list:
        #Step 1 - Get the ID from transcript_list API
        #transcripts_list API returns a dictionary 
        #transcripts is the lookup value for the dictionary and the result is an array
        result_transcript_list=[]

        for retry in range(max_tries):
            try:
                ticker=ticker.strip()
                result_transcript_list= finnhub_client.transcripts_list(ticker)['transcripts']
                break
            except:
                print ("Error downloading: " + ticker + " retry# " + str(retry)  )
                #Sleep for x seconds and try again
                time.sleep(45)
        
        id_list=filtered_result(result_transcript_list, processed_filename)
        
        #Step 2 - Download the transcript from transcripts API
        for id in id_list:
            for retry in range(max_tries):
                try:
                    result_transcript=finnhub_client.transcripts(id)
                    break
                except Exception as e:
                    print ("Error downloading:" + id + "retry# " + str(retry)  )
                    #Sleep for x seconds and try again
                    time.sleep(45)     
                
            write_file(json.dumps(result_transcript), os.path.join(output_text_dir,id))
            write_file(str(id) + ",", processed_filename) 
        
        print("Downloaded ticker: " + ticker + "# " + str(count_downloaded) + " of " + str(len(ticker_list)))
        count_downloaded=count_downloaded+1

       


if __name__ == '__main__':
    finnhub_client = finnhub.Client(api_key="btrldbv48v6vjo68mf30")
    russell_output="/Users/shankar.sivadasan/Downloads/Transcripts_results/Russell/Transcripts_Russell"
    sp_output="/Users/shankar.sivadasan/Downloads/Transcripts_1500"
    russell_ticker="/Users/shankar.sivadasan/Downloads/Transcripts_results/Russell/Russell3000.csv"
    sp_ticker="/Users/shankar.sivadasan/Downloads/Transcripts_results/SP_1500.csv"
    processed_filename="/Users/shankar.sivadasan/Downloads/Transcripts_results/Russell/processed_Russell.csv"
    """     ticker_list=['MMM',  'ABT',  'ABBV',  'ABMD',  'ACN',  'ATVI',  'ADBE',  'AMD',  'AAP',  'AES',  'AFL',  'A',  'APD',  'AKAM',  'ALK',  
    'ALB',  'ARE',  'ALXN',  'ALGN',  'ALLE',  'LNT',  'ALL',  'GOOGL',  'GOOG',  'MO',  'AMZN',  'AMCR',  'AEE',  'AAL',  'AEP',  'AXP',  
    'AIG',  'AMT',  'AWK',  'AMP',  'ABC',  'AME',  'AMGN',  'APH',  'ADI',  'ANSS',  'ANTM',  'AON',  'AOS',  'APA',  'AAPL',  'AMAT',  
    'APTV',  'ADM',  'ANET',  'AJG',  'AIZ',  'T',  'ATO',  'ADSK',  'ADP',  'AZO',  'AVB',  'AVY',  'BKR',  'BLL',  'BAC',  'BK',  'BAX',  
    'KALU',  'KAMN',  'KELYA',  'KFY',  'KLIC',  'KN',  'KOP',  'KRA',  'KREF',  'KRG',  'KTB',  'KWR',  'LCI',  'LCII',  'LDL',  'LGIH',  
    'LL',  'LMAT',  'LMNX',  'LNN',  'LNTH',  'LOCO',  'LPG',  'LPI',  'LPSN',  'LQDT',  'LTC',  'LTHM',  'LXP',  'LZB',  'M',  'MANT']  """
  
    ticker_list=getcsv(russell_ticker, sp_ticker, russell_output, sp_output)
    #process_transcripts(ticker_list, finnhub_client, russell_output, processed_filename)

