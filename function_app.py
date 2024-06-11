import azure.functions as func
import logging
import os #in order to get parameters values from azure function app enviroment vartiable - sql password for example 
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient # in order to use azure container storage
from PyPDF2 import PdfReader,PdfWriter  # in order to read and write  pdf file 
import io # in order to download pdf to memory and write into memory without disk permission needed 
import json # in order to use json 
import pyodbc #for sql connections 
from azure.servicebus import ServiceBusClient, ServiceBusMessage # in order to use azure service bus 
import uuid #using for creating unique name to files 
from azure.data.tables import TableServiceClient, TableClient # in order to use azure storage table  
from azure.core.exceptions import ResourceExistsError # in order to use azure storage table  

# Azure Blob Storage connection string & key 
connection_string_blob = os.environ.get('BlobStorageConnString')
#Azure service bus connection string 
connection_string_servicebus = os.environ.get('servicebusConnectionString')

# Define connection details
server = 'medicalanalysis-sqlserver.database.windows.net'
database = 'medicalanalysis'
username = os.environ.get('sql_username')
password = os.environ.get('sql_password')
driver= '{ODBC Driver 18 for SQL Server}'



#Create event on azure service bus 
def create_servicebus_event(queue_name, event_data):
    try:
        # Create a ServiceBusClient using the connection string
        servicebus_client = ServiceBusClient.from_connection_string(connection_string_servicebus)

        # Create a sender for the queue
        sender = servicebus_client.get_queue_sender(queue_name)

        with sender:
            # Create a ServiceBusMessage object with the event data
            message = ServiceBusMessage(event_data)

            # Send the message to the queue
            sender.send_messages(message)

        print("Event created successfully.")
    
    except Exception as e:
        print("An error occurred:", str(e))

def batching_pdf_pages(caseid,file_name):
    try:
        logging.info(f"split_pdf_pages caseid value is: {caseid} ")
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid

        # Initialize Blob Service Client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)

        # Construct the full path
        basicPath = f"{main_folder_name}/{folder_name}"
        path = f"{basicPath}/source/{file_name}"
        logging.info(f"full path is : {path}")

        # Get blob client
        blob_client = container_client.get_blob_client(path)


        # Check if the file exists
        fileExist = blob_client.exists()
        logging.info(f"fileExist value is: {fileExist}")
        if fileExist==False:
           return "not found file"
        
        # Download the blob into memory
        download_stream = blob_client.download_blob()
        pdf_bytes = download_stream.readall()

        # Open the PDF from memory
        pdf_file = io.BytesIO(pdf_bytes)  

        # Create PdfFileReader object
        pdf_reader = PdfReader(pdf_file)

        # Get number of pages
        num_pages = len(pdf_reader.pages)
        logging.info(f"num_pages value: {num_pages}")

        # Log start and end pages for segments of up to 50 pages
        for start_page in range(0, num_pages, 50):
            end_page = min(start_page + 49, num_pages - 1)
            logging.info(f"Start Page: {start_page + 1}, End Page: {end_page + 1}")

            #preparing data for service bus 
            data = { 
                    "caseid" : caseid, 
                    "filename" :file_name,
                    "Subject" : "Case created successfully!" ,
                    "start_page": (start_page + 1),
                    "end_page" : (end_page + 1)
                } 
            json_data = json.dumps(data)
            create_servicebus_event("split",json_data)
        return "Done"
    except Exception as e:
           return "issues"
    
#  Function checks if this is a duplicate request for split operation 
def check_duplicate_request(caseid):
    try:
        logging.info(f"starting check_duplicate_request")
        container_name = "medicalanalysis"
        main_folder_name = "cases"
        folder_name="case-"+caseid
        blob_service_client = BlobServiceClient.from_connection_string(connection_string_blob)
        container_client = blob_service_client.get_container_client(container_name)
        basicPath = f"{main_folder_name}/{folder_name}"
        directory_path = f"{basicPath}/source/split"
        # List blobs in the specified directory
        blobs = container_client.list_blobs(name_starts_with=directory_path)
        # Count the number of files in the directory
        file_count = sum(1 for _ in blobs)
        logging.info(f"check_duplicate_request, total files in the path: {directory_path}, is: {file_count}")
        if file_count>0:
           return True
        return False 
    except Exception as e:
        logging.error(f"Error update case: {str(e)}")
        return True    

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="batch-pages-splitter",
                               connection="medicalanalysis_SERVICEBUS") 
def BatchPagesSplitter(azservicebus: func.ServiceBusMessage):
    message_data = azservicebus.get_body().decode('utf-8')
    logging.info('Received messageesds: %s', message_data)
    message_data_dict = json.loads(message_data)
    caseid = message_data_dict['caseid']
    file_name = message_data_dict['filename']
    duplicateStatus =  check_duplicate_request(caseid)
    logging.info(f"duplicateStatus check is : {duplicateStatus}")
    if duplicateStatus==False:
        logging.info(f"starting proccesing")
        batching_pdf_pages(caseid,file_name)
    else:
        logging.info(f"duplicate Status is True - means the process already made")