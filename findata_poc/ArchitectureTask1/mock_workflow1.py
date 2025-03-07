import time
import datetime
import uuid
import random
import zipfile
import json
import os
import threading  # For simulating asynchronous tasks

# Mock Kafka (in-memory)
workflow_topic = []
logging_topic = []
zip_filepath_topic = []
json_filepath_topic = []
processing_status = {}
mock_s3 = {}

# Consumer Group Name
ETL_CONSUMER_GROUP = "ETLWorkers"

#----------------------------------------------------------------------
# 1. Scheduling & Outbound Request Module (Updated)
#----------------------------------------------------------------------
class Scheduler:
    def __init__(self, partner_api_url, request_id):  # Takes request_id
        self.partner_api_url = partner_api_url
        self.daily_request_made = False
        self.lock = threading.Lock()
        self.request_id = request_id # Holds the current request_id

    def schedule_daily_request(self):
        """Simulates a daily scheduled task."""
        while True:  # Run indefinitely
            now = datetime.datetime.utcnow()
            # Check if it's 4 PM UTC (11 AM EST)
            if now.hour == 16 and now.minute == 0 and not self.daily_request_made:
                self.make_request()
                with self.lock:
                    self.daily_request_made = True
                print("Daily request triggered.")

            elif now.hour != 16:
                with self.lock:
                    self.daily_request_made = False  # Reset after the day passes
            time.sleep(60)  # Check every minute


    def validate_request(self):
        """Mocks request validation (rate limiting)."""
        with self.lock:
            if self.daily_request_made:
                print("Request blocked: Only one request allowed per day.")
                return False
            else:
                return True

    def make_request(self):
        """Simulates making an API request to the partner."""
        if self.validate_request():
            timestamp = datetime.datetime.utcnow().isoformat()

            print(f"Making request to {self.partner_api_url}...")

            # Simulate API call (replace with actual API call)
            # Assuming partner returns 201 Created on success.  Here, just simulate.
            response_code = 201
            if response_code == 201:
                print("Received 201 Created.")
                self.enqueue_workflow_message() # No parameters as it uses self.request_id
                self.log_status("Request sent", timestamp)
                with self.lock:
                    self.daily_request_made = True
            else:
                print(f"Request failed with code: {response_code}")
                self.log_status(f"Request failed with code: {response_code}", timestamp)


    def enqueue_workflow_message(self):  #uses self.request_id
        """Mocks enqueueing a message onto the workflow Kafka topic."""
        message = {
            "module": "Scheduler",
            "request_id": self.request_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "status": "Request Initiated",
        }
        workflow_topic.append(message)
        print(f"Enqueued to workflow topic: {message}")

    def log_status(self, message, timestamp): # uses self.request_id
        """Mocks logging to the observability Kafka topic."""
        log_message = {
            "module": "Scheduler",
            "request_id": self.request_id,
            "timestamp": timestamp,
            "message": message,
        }
        logging_topic.append(log_message)
        print(f"Logged: {log_message}")

#----------------------------------------------------------------------
# 2. Webhook Listener Module (Updated)
#----------------------------------------------------------------------
class WebhookListener:
    def __init__(self, request_id):  # Takes request_id
        self.request_id = request_id
        self.partner_secrets = {
            "partner_secret_1": "PartnerA",
            "partner_secret_2": "PartnerB",
        }

    def receive_webhook(self, request):
        """Mocks receiving a webhook call."""
        zip_file_url = request.get("zip_file_url")
        partner_secret = request.get("partner_secret")

        partner_id = self.lookup_partner_id(partner_secret)

        if not partner_id:
            print("Webhook validation failed: Invalid Partner Secret.")
            return "Webhook validation failed: Invalid Partner Secret", 403

        if not self.validate_webhook(partner_id): #Uses self.request_id and takes only partnerId as parameter
            return "Webhook validation failed", 403

        if zip_file_url:
            print(f"Received webhook from Partner {partner_id} with zip file URL: {zip_file_url}")
            self.enqueue_file_download(zip_file_url, partner_id) # No request_id parameter but partner ID is still included
            self.update_workflow_topic("Webhook Received") #No request_id parameter as it uses self.request_id
            self.log_status(f"Webhook received and enqueued download from Partner {partner_id}")
            return "Webhook received", 200
        else:
            self.log_status(f"Webhook received from Partner {partner_id} but missing zip_file_url")
            return "Missing zip_file_url", 400

    def lookup_partner_id(self, partner_secret):
        """Looks up the partner ID based on the secret key."""
        return self.partner_secrets.get(partner_secret)

    def validate_webhook(self, partner_id): #Uses self.request_id
        """Mocks webhook validation."""
        # Check if the request_id matches the expected value (from the scheduler)
        if not partner_id:
            print("Webhook validation failed: Invalid Partner ID.")
            return False
        print(f"Webhook validated for Request: {self.request_id}.")
        return True

    def enqueue_file_download(self, zip_file_url, partner_id):  #Uses self.request_id, no request_id as a parameter
        """Mocks enqueueing the zip file URL for download."""
        message = {"zip_file_url": zip_file_url, "request_id": self.request_id, "partner_id": partner_id}
        zip_filepath_topic.append(message)
        print(f"Enqueued to zip_filepath_topic: {message}")

    def update_workflow_topic(self, status):  #Uses self.request_id, no request_id as a parameter
        """Mocks updating the workflow Kafka topic."""
        message = next((item for item in workflow_topic if item["request_id"] == self.request_id), None)
        if message:
            message["status"] = status
            print(f"Updated workflow topic: {message}")

    def log_status(self, message):   #Uses self.request_id, no request_id as a parameter
        """Mocks logging to the observability Kafka topic."""
        log_message = {
            "module": "WebhookListener",
            "request_id": self.request_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "message": message,
        }
        logging_topic.append(log_message)
        print(f"Logged: {log_message}")

#----------------------------------------------------------------------
# 3. File Download/Storage/Extraction Module (Updated)
#----------------------------------------------------------------------
class FileProcessor:
    def __init__(self, s3_bucket="my-s3-bucket", request_id=None):  # Takes request_id
        self.s3_bucket = s3_bucket
        self.request_id = request_id

    def process_file_message(self, message):
        """Processes a message from the file Kafka topic."""
        if message["request_id"] != self.request_id:
            print(f"Skipping message (wrong request_id): {message}")
            return

        zip_file_url = message["zip_file_url"]
        partner_id = message["partner_id"]

        try:
            zip_file_path = self.download_file(zip_file_url)
            extracted_files = self.extract_files(zip_file_path)
            s3_file_paths = self.upload_to_s3(extracted_files)

            for file_path in s3_file_paths:
                self.publish_file_path(file_path, partner_id) # Uses request_id from constructor but includes PartnerID

            self.update_workflow_topic("Files Downloaded, Extracted, and Uploaded")
            self.log_status("Files processed successfully")

        except Exception as e:
            print(f"Error processing file: {e}")
            self.log_status(f"Error processing file: {e}")
            self.update_workflow_topic(f"File Processing Failed: {e}")

    def download_file(self, zip_file_url):
        """Mocks downloading the zip file."""
        print(f"Downloading file from {zip_file_url}...")
        # Simulate network issues
        if random.random() < 0.1:  # 10% chance of failure
            raise Exception("Network error during download")
        time.sleep(1)  # Simulate download time
        return "downloaded_data.zip"  # Mock path

    def extract_files(self, zip_file_path):
        """Mocks extracting JSON files from the zip file."""
        print(f"Extracting files from {zip_file_path}...")
        # Create a dummy zip file for testing
        self.create_dummy_zip_file("downloaded_data.zip", num_files=3)  # Create on first call

        extracted_files = []
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                extracted_files = [f"extracted/{name}" for name in zip_ref.namelist()] # Simulate files

            # Simulate errors
            if random.random() < 0.05:  # 5% chance of corrupted zip
                raise Exception("Corrupted zip file")

            # Simulate files extracted successfully.
            print(f"Extracted files: {extracted_files}")
            return extracted_files
        except zipfile.BadZipFile as e:
            raise Exception(f"Error extracting zip file: {e}")

    def create_dummy_zip_file(self, zip_file_name, num_files=3):
        """Creates a dummy zip file with JSON files."""
        if os.path.exists(zip_file_name): # Only create if it doesn't exist.
            return  # File already exists

        os.makedirs("extracted", exist_ok=True)  # Create directory
        with zipfile.ZipFile(zip_file_name, 'w') as zip_file:
            for i in range(num_files):
                file_name = f"client_{i}.json"
                file_path = os.path.join("extracted", file_name)
                with open(file_path, 'w') as f:
                    json.dump({"client_id": i, "account_balance": random.randint(1000, 100000)}, f)
                zip_file.write(file_path, file_name)
                os.remove(file_path)  # Clean up individual files

    def upload_to_s3(self, file_paths):
        """Mocks uploading files to S3."""
        s3_file_paths = []
        for file_path in file_paths:
            s3_path = f"s3://{self.s3_bucket}/{file_path}"
            print(f"Uploading {file_path} to {s3_path}...")
            mock_s3[s3_path] = f"Contents of {file_path}"  # Simulate upload
            s3_file_paths.append(s3_path)
        return s3_file_paths

    def publish_file_path(self, file_path, partner_id): # Uses request_id from constructor but includes PartnerID
        """Mocks publishing a file path to the processing topic."""
        message = {"file_path": file_path, "request_id": self.request_id, "partner_id": partner_id}
        json_filepath_topic.append(message)
        print(f"Published to json_filepath_topic: {message}")

    def update_workflow_topic(self, status):   #Uses self.request_id, no request_id as a parameter
        """Mocks updating the workflow Kafka topic."""
        message = next((item for item in workflow_topic if item["request_id"] == self.request_id), None)
        if message:
            message["status"] = status
            print(f"Updated workflow topic: {message}")

    def log_status(self, message):  #Uses self.request_id, no request_id as a parameter
        """Mocks logging to the observability Kafka topic."""
        log_message = {
            "module": "FileProcessor",
            "request_id": self.request_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "message": message,
        }
        logging_topic.append(log_message)
        print(f"Logged: {log_message}")

#----------------------------------------------------------------------
# 4. ETL Module 
#----------------------------------------------------------------------
class ETLWorker:
    def __init__(self, request_id): # Takes request_id
        self.request_id = request_id # Tie component to a request_id
        self.consumer_group = ETL_CONSUMER_GROUP # all workers from the same group.
    def process_file(self, message):
        """Processes a single JSON file."""
        if message["request_id"] != self.request_id:
            print(f"Skipping message (wrong request_id): {message}")
            return

        file_path = message["file_path"]
        partner_id = message["partner_id"]
        message_id = str(uuid.uuid4())

        try:
            if self.is_processing(file_path):
                print(f"File {file_path} is already being processed. Skipping.")
                return

            self.set_processing_status(message_id, file_path, "processing")

            file_contents = self.read_file(file_path)
            data = json.loads(file_contents)
            self.validate_data(data)
            transformed_data = self.transform_data(data)
            self.write_to_database(transformed_data, partner_id, message["request_id"])  # Pass request_id

            self.set_processing_status(message_id, file_path, "completed")
            self.update_workflow_topic(f"File {file_path} ETL Completed")
            self.log_status(f"File {file_path} ETL completed")

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            self.log_status(f"Error processing {file_path}: {e}")
            self.set_processing_status(message_id, file_path, "failed")
            self.update_workflow_topic(f"File {file_path} ETL Failed: {e}")

    def read_file(self, file_path):
        """Mocks reading the file from S3."""
        print(f"Reading file from {file_path}...")
        # Simulate reading from S3
        if file_path in mock_s3:
            return mock_s3[file_path]
        else:
            raise Exception(f"File not found in S3: {file_path}")

    def validate_data(self, data):
        """Mocks validating the JSON data."""
        print(f"Validating data: {data}...")
        # Simulate schema validation, required fields, etc.
        if not isinstance(data, dict) or "client_id" not in data or "account_balance" not in data:
            raise ValueError("Invalid data format")

    def transform_data(self, data):
        """Mocks transforming the data."""
        print(f"Transforming data: {data}...")
        # Simulate data transformations (normalization, mapping, etc.)
        transformed_data = {
            "client_id": data["client_id"],
            "account_balance": float(data["account_balance"]),
            "processed_at": datetime.datetime.utcnow().isoformat(),
        }
        return transformed_data

    def write_to_database(self, data, partner_id, request_id): # Added request_id as parameter
        """Mocks writing data to the database."""
        print(f"Writing data to database: {data} from Partner {partner_id}...")
        # Simulate database insertion.  Implement Idempotency here.
        client_id = data["client_id"]
        # Simulate the unique constraint
        unique_key = f"{request_id}-{partner_id}-{client_id}-{datetime.date.today()}" #unique_key is now request_id aware

        # Check if this client's data for today has already been written
        if unique_key in mock_db:
            print(f"Data for client {client_id} from Partner {partner_id} on {datetime.date.today()} already exists. Skipping write.")
            return  # Skip the write, maintaining idempotency

        mock_db[unique_key] = data  # Store the data in the mock database
        print(f"Data written for client {client_id} from Partner {partner_id}.")

    def is_processing(self, file_path):
        """Checks if the file is currently being processed - this needs to be done on database!! below is mock impl"""
        for message_id, status in processing_status.items():
            if status["file_path"] == file_path and status["status"] == "processing":
                return True
        return False

    def set_processing_status(self, message_id, file_path, status):
        """Mocks updating the processing status table. this needs to be done on database!! below is mock impl"""
        processing_status[message_id] = {
            "file_path": file_path,
            "status": status,
            "last_updated": datetime.datetime.utcnow().isoformat(),
        }
        print(f"Updated processing status for {file_path}: {status}")

    def update_workflow_topic(self, status): #Uses self.request_id, no request_id as a parameter
        """Mocks updating the workflow Kafka topic."""
        message = next((item for item in workflow_topic if item["request_id"] == self.request_id), None)
        if message:
            message["status"] = status
            print(f"Updated workflow topic: {message}")

    def log_status(self, message): #Uses self.request_id, no request_id as a parameter
        """Mocks logging to the observability Kafka topic."""
        log_message = {
            "module": "ETLWorker",
            "request_id": self.request_id,
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "message": message,
        }
        logging_topic.append(log_message)
        print(f"Logged: {log_message}")

#----------------------------------------------------------------------
# 5. Monitoring, Logging & Alerting Module (No Changes)
#----------------------------------------------------------------------
class Monitor:
    def __init__(self, error_threshold=3, request_id = None):
        self.error_threshold = error_threshold
        self.error_count = 0
        self.request_id = request_id

    def check_logs(self):
      """Simulates checking logs for errors and anomalies."""
      # Only check logs related to the current request_id
      errors = [log for log in logging_topic if "Error" in log["message"] and log["request_id"] == self.request_id]

      if errors:
          self.error_count += len(errors)
          print(f"Found {len(errors)} errors in logs (request_id: {self.request_id}).")
          for error in errors:
              print(f"  - {error}")
      else:
          print(f"No errors found in logs for request_id: {self.request_id}.")

      if self.error_count > self.error_threshold:
          self.trigger_alert(f"Error count exceeded threshold ({self.error_threshold})")
          self.error_count = 0  # Reset counter after alerting.

    def check_workflow(self):
        """Simulates checking the workflow topic for stalled processes."""
        incomplete_workflows = [
            item for item in workflow_topic if item["status"] != "Files Downloaded, Extracted, and Uploaded" and item["status"] != "File Processing Completed"  and item["request_id"] == self.request_id
        ]
        if incomplete_workflows:
            print(f"Found {len(incomplete_workflows)} incomplete workflows for request_id: {self.request_id}.")
            for workflow in incomplete_workflows:
                print(f"  - Request ID: {workflow['request_id']}, Status: {workflow['status']}")
                #Potentially trigger alerts for stalled workflows.
        else:
            print(f"No incomplete workflows found for request_id: {self.request_id}.")

    def trigger_alert(self, message):
        """Mocks triggering an alert (e.g., sending an email or Slack message)."""
        print(f"ALERT: {message}")
        # In a real system, you would send an email, Slack notification, etc.
        # using libraries like `smtplib` or `slack_sdk`.
#----------------------------------------------------------------------
# Main Execution
#----------------------------------------------------------------------

# Mock database (in-memory)
mock_db = {}

def main():
    #1.  Initialization
    request_id = str(uuid.uuid4())  # Create RequestID in Main
    partner_api_url = "https://partner.com/api/request_accounts"

    # Pass request_id to all components
    scheduler = Scheduler(partner_api_url, request_id)
    webhook_listener = WebhookListener(request_id)
    file_processor = FileProcessor(request_id=request_id)
    etl_worker = ETLWorker(request_id)
    monitor = Monitor(request_id=request_id)

    # Set up the scheduler thread
    scheduler_thread = threading.Thread(target=scheduler.schedule_daily_request, daemon=True)
    scheduler_thread.start()

    #2. Simulate the Workflow with some delay
    # Simulate webhook call after a delay
    time.sleep(5)  # Wait a few seconds for the scheduler to (potentially) run

    # Simulate receiving the webhook - needs a request ID, which comes from the scheduler module.
    webhook_request = {
        "zip_file_url": "https://partner.com/data/client_data.zip",
        "request_id": request_id,
        "partner_secret": "partner_secret_1",
    }
    webhook_listener.receive_webhook(webhook_request)

    # Simulate file processing after another delay
    time.sleep(5)

    #Filter zip filepath topic messages to process
    filtered_zip_filepath_topic = [msg for msg in zip_filepath_topic if msg["request_id"] == request_id]

    if filtered_zip_filepath_topic:
        file_message = filtered_zip_filepath_topic.pop(0)  # Get the first message from the file topic
        file_processor.process_file_message(file_message)

    # Simulate ETL processing after another delay
    time.sleep(5)

   # Ensure ETL Worker only picks up the work for this request ID
    filtered_json_filepath_topic = [msg for msg in json_filepath_topic if msg["request_id"] == request_id]

    # Simulate ETL processing after another delay
    time.sleep(5)
    # Ensure ETL Worker only picks up the work for this request ID

    #For a Consumer Group implementation you would need to make your python threads aware
    # that they all belong to same Consumer Group
    #This involves using a librar such as confluent-kafka to spin threads

    # Simulate multiple ETL workers in the same consumer group:
    for _ in range(len(filtered_json_filepath_topic)):
        file_message = filtered_json_filepath_topic.pop(0)
        etl_worker.process_file(file_message)


    # Simulate monitoring after processing
    time.sleep(2)
    print("\n--- Monitoring ---\n")
    monitor.check_logs()
    monitor.check_workflow()

if __name__ == "__main__":
    main()