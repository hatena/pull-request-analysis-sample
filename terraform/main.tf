variable "google_credentials" {
  type    = string
  default = "{}"
}

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.51.0"
    }
  }

  backend "remote" {
    hostname     = "app.terraform.io"
    organization = "hatena"

    workspaces {
      name = "pull-request-analysis-sample"
    }
  }
}

provider "google" {
  credentials = var.google_credentials
  project     = "pull-request-analysis-sample"
  region      = "asia-northeast1"
}

resource "google_service_account" "github_importer" {
  account_id   = "github-importer"
  display_name = "github-importer"
  description  = "GitHubの情報をBigQueryに流し込むためのサービスアカウント"
}
resource "google_project_iam_member" "github_importer_bigquery_jobuser_bindings" {
  role   = "roles/bigquery.jobUser"
  member = "serviceAccount:${google_service_account.github_importer.email}"
}
resource "google_bigquery_dataset_access" "github_importer_bigquery_dataset_access_bindings" {
  dataset_id    = "source__github"
  role          = "WRITER"
  user_by_email = google_service_account.github_importer.email
}

resource "google_service_account" "dataform_executor" {
  account_id  = "dataform-executor"
  description = "Datafrom (dataform.co) に付与する用のサービスアカウント"
}
resource "google_project_iam_member" "dataform_executor_bigquery_admin_bindings" {
  role   = "roles/bigquery.admin"
  member = "serviceAccount:${google_service_account.dataform_executor.email}"
}
