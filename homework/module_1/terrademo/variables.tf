variable "project" {
  description = "Project"
  default     = "speedy-method-485303-p9"
}

variable "credentials" {
  description = "Credentials"
  default     = "./keys/my-creds.json"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}
variable "bq_dataset_name" {
  description = "Zoomcamp BigQuery"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Zoomcamp Bucket Name"
  default     = "speedy-method-485303-p9-demo-terraform"
}

variable "gcs_storage_class" {
  description = "Zoomcamp Bucket Storage Class"
  default     = "STANDARD"
}