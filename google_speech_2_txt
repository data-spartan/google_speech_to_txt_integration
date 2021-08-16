#!/root/google_STT/env/bin/python3
# Import libraries
import os
from google.cloud import speech
from google.cloud import storage
import json
import sys
import logging
import logging.config

####################################################################
#logging mechanism with config logging file
logging.config.fileConfig('/root/google_STT/google_speech_2_tx/conf.log/logging.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)
logger.info('Script started...')

#####################################################################
#arguments block
filepath = sys.argv[1]     #input file path
output_path = sys.argv[2]  #transcribe output
source_file = sys.argv[3]  #input file
bucketname=sys.argv[4]
######################################################################

#uploads file to the bucket
def upload_blob(bucket_name, source_filepath, destination_blob_name):
    try:
        logger.info('Upload started...')
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_filepath)
    except:
        logger.error('Incorrect path to the source file-upload failed')
    finally:
        logger.info('Upload finished...')

#deletes file from cloud
def delete_blob(bucket_name, blob_name):
    try:
        logger.info('Started blob deletion...')
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
    except:
        logger.error('Deletion went wrong...')
    finally:
        logger.info('Blob deletion finished...')
    
#google speech2txt output is very different from daktilograf output, data formating is needed
def data_formating(unformated):
    try:    
        logger.info('Data formating started...')
        result={'transcripts':[{'confidence':[],'words':[]}]}
        for i in range(len(unformated['results'])):
            result['transcripts'][0]['words']+=unformated['results'][i]['alternatives'][0]['words']
            result['transcripts'][0]['confidence'].append(str(unformated['results'][i]['alternatives'][0]['confidence']))
            for n in unformated['results'][i]['alternatives'][0]['words']:
                n['start_time']=round(float(n['start_time'][:-1]),3)
                n['end_time']=round(float(n['end_time'][:-1]),3)
                n['duration']=round((n['end_time']-n['start_time']),3)
                del n['speaker_tag']
                del n['end_time']
        for i in range(len(result['transcripts'][0]['confidence'])):
            if i < len(result['transcripts'][0]['confidence']) - 1:
                result['transcripts'][0]['confidence'][i] =result['transcripts'][0]['confidence'][i]
            else:
                result['transcripts'][0]['confidence']=round(sum(map(float,result['transcripts'][0]['confidence']))/len(result['transcripts'][0]['confidence']),5)
    except:
        logger.error('Error occured-incompatible google output format or type,check google_speech_2_txt output format or type')
    finally:
        logger.info('Data formating finished...')
    return result

#writes to .json file format
def write_json(data):
    try:
        logger.info('Writing started...')
        with open(output_path + source_file.split('.')[0] + '.json','w', encoding='utf8') as file:
            json.dump(data,file,ensure_ascii=False)
    except:
        logger.error('Output file path doesnt exist...')
    finally:
        logger.info('Writing finished/Script execution finished...')

def transcribe(source_file):
    try:
        logger.info('Transcribing process started...')
        source_filepath = filepath + source_file
        upload_blob(bucketname, source_filepath, source_file)
        gcs_uri = 'gs://' + bucketname + '/' + source_file

        client = speech.SpeechClient()

        audio = speech.RecognitionAudio(uri=gcs_uri)
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
            language_code="bs-BA",
            enable_word_time_offsets=True,
            audio_channel_count=2,
        )

        operation = client.long_running_recognize(config=config, audio=audio)

        logger.info('Recognition started...')
        result = operation.result(timeout=1000000)

        #converts to dict type in order to apply data formating
        response_dict = type(result).to_dict(result)
        delete_blob(bucketname,source_file)
        return response_dict
    except:
        logger.error('Error occured-operation exceded timeout...')
    finally:
        logger.info('Response came in...')

#execution block
if __name__ == "__main__":
    unformated = transcribe(source_file)
    transcript = data_formating(unformated)
    write_json(transcript)

