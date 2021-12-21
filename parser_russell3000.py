import os
import json
import time
import pandas as pd
import csv
import traceback, sys



def write_csv(arraytowrite, output_path):
    output_file= open(output_path,'a')
    csvwriter=csv.writer(output_file, delimiter="|")
    if isinstance(arraytowrite[0],list):
        csvwriter.writerows(arraytowrite)
    else:
        csvwriter.writerow(arraytowrite)
    output_file.close()



def arraytostring(array):
    final_string=" "
    return final_string.join(array)


def extract_metadata(transcript):

    metdata_info= [transcript["id"], transcript["symbol"], transcript["quarter"], transcript["time"],transcript["title"], transcript["year"]]
    return metdata_info

def extract_speech(id, symbol, speeches):
    speech_info=[]
    order=0

    for speech in speeches:
        speech_info.append([id, symbol, order, speech["name"], arraytostring(speech["speech"]), speech["session"]])
        order=order+1
    return speech_info

def extract_participant(id, symbol, participants):
    participant_info=[]

    for participant in participants:
        participant_info.append([id, symbol, participant["name"], participant["description"], participant["role"]])
    return participant_info

def extract_text (input_path, metadata_file, participant_file, speech_file):
    input_file=open(input_path) 
    try:
        transcript = json.load(input_file) 
        id=transcript["id"]
        symbol=transcript["symbol"]
        participant_info=extract_participant(id, symbol, transcript["participant"])
        write_csv(participant_info, participant_file)    

        
        speech_info=extract_speech(id, symbol, transcript["transcript"])
        write_csv(speech_info, speech_file) 
        
        metadata_info=extract_metadata(transcript)
        write_csv(metadata_info,metadata_file)

     
    except Exception as e:
        print("Error loading file: " + str(input_path))
        traceback.print_exception(*sys.exc_info())
        print(e)
        pass  
    input_file.close()
 

def main():
    input_dir ="/Users/shankar.sivadasan/Downloads/Transcripts_1500"
    participant_file="/Users/shankar.sivadasan/Downloads/Transcripts_results/participant.csv"
    speech_file="/Users/shankar.sivadasan/Downloads/Transcripts_results/speech.csv"
    metadata_file="/Users/shankar.sivadasan/Downloads/Transcripts_results/metadata.csv"

    #Write the headers for the CSV files
    header_participant=['id', 'symbol', 'name', 'description', 'role']
    write_csv(header_participant, participant_file)

    header_speech=['id', 'symbol', 'order', 'name', 'speech', 'session']
    write_csv(header_speech, speech_file)

    header_metadata=['id', 'symbol', 'quarter', 'time', 'title', 'year']
    write_csv(header_metadata, metadata_file)
    counter=0
    for root, dirs, files in os.walk(input_dir):
        for f in files:
            input_path = os.path.join(root, f)
            extract_text(input_path,metadata_file, participant_file, speech_file)
            print("Processed file # " + str(counter))
            counter=counter+1

if __name__ == '__main__':
    main()

