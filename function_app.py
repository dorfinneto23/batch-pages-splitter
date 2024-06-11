import azure.functions as func
import logging

app = func.FunctionApp()

@app.service_bus_queue_trigger(arg_name="azservicebus", queue_name="batch-pages-splitter",
                               connection="medicalanalysis_SERVICEBUS") 
def BatchPagesSplitter(azservicebus: func.ServiceBusMessage):
    logging.info('Python ServiceBusv Queue trigger processed a message: %s',
                azservicebus.get_body().decode('utf-8'))
