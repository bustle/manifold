![manifold](https://github.com/bustle/manifold/assets/8030836/be7f24f6-27b9-4924-8e1a-ae2899c7b272)

# Manifold

![CI](https://github.com/bustle/manifold/actions/workflows/ci.yml/badge.svg)

Manifold is a framework designed to streamline the process of generating and managing data infrastructures in Google BigQuery using Terraform. By leveraging Manifold, teams can efficiently create complex views that join dimensional data with multiple metrics tables, enabling more dynamic data interactions and fewer sandcastles.

## Philosophy

At the heart of Manifold, our philosophy is to simplify the complexity inherent in managing large-scale data architectures. We aim to provide a tool that not only facilitates the easy setup of data structures but also adheres to best practices in scalability, maintainability, and performance. Manifold is built for data engineers, by data engineers, ensuring that the nuances and common challenges in data management are well-addressed.

## Features

- **Unified Data Modeling**: Manifold introduces a standardized way to model dimensions and metrics, ensuring consistency and reliability in data reporting and analysis.
- **Scalability**: Designed to handle large volumes of data, supporting a variety of data types and structures.
- **Flexibility**: Easily adapt to different kinds of metric groupings such as by device type (e.g., desktop, tablet, mobile) with identical metric structures beneath these segmentations.

## Getting Started

### Prerequisites

- Ruby
- Terraform
- Google Cloud SDK (gcloud)

### Installation

1. **Install the Manifold Gem**:
   `manifold` is distributed as a Ruby gem. To install it, run:

```bash
gem install manifold
```

2. **Setup Terraform**: Ensure that Terraform is installed and configured to interact with your Google Cloud Platform account.

3. **Configure Your Environment**: Set up your environment variables and credentials to access Google BigQuery and other necessary services.

## Usage

1. **Initialize a New Umbrella Project**

Set up a new umbrella project directory with the necessary structure for managing multiple data projects.

```bash
manifold init <project_name>
```

2. **Add a New Data Project**

Add a new data project under the umbrella. This setup includes creating a directory for the data project and initializing with a template `manifold.yml` file.

```bash
cd <project_name>
manifold add <data_project_name>
```

3. **Generate BigQuery Resource Definitions and Terraform Configuration**

After you fill out the manifold.yml file, this command generates the necessary BigQuery schema files and Terraform configurations based on the specified dimensions and metrics.

```bash
# Generate with submodule configuration (default)
manifold generate

# Generate with provider configuration (if needed)
manifold generate --no-submodule
```

This will create:

- A root `main.tf.json` file that sets up workspace modules (by default) or includes provider configuration (with --no-submodule)
- Individual workspace configurations in `workspaces/<workspace_name>/main.tf.json`
- Dataset and table definitions that reference your generated BigQuery schemas

By default, the generated Terraform configurations are designed to be included as a module in your larger Terraform configuration:

```hcl
module "my_manifold_project" {
  source = "./path/to/manifold/project"
  project_id = var.project_id
}
```

If you need to use the configuration standalone (with `--no-submodule`), you can apply it directly:

```bash
terraform init
terraform plan -var="project_id=your-project-id"
terraform apply -var="project_id=your-project-id"
```

## Manifold Configuration

### Vectors

Vectors are the entities you can roll up data for. Each vector has a set of dimensions defined in its `vectors/<vector_name>.yml` configuration file.

```yaml
vectors:
  - page
```

#### Add a vector to your project

```bash
manifold vectors add page
```

### Metrics

Metrics are fields that contain numerical data that can be aggregated. They are typically used to measure performance or other quantitative data.

#### Example

```yaml
metrics:
  - name: Pageviews
    id:
      field: pageId
      type: STRING
    interval:
      type: TIMESTAMP
      expression: TIMESTAMP_TRUNC(timestamp, HOUR)
    aggregations:
      - name: pageviews
        method: count
      - name: sessions
        method: distinct
        field: sessionid
    source:
      type: bigquery
      name: Events.Requests
      breakouts:
        - name: us
          condition: CountryId = 2840
```

- _Name_: The name of the metric.
- _ID_: The field that uniquely identifies the metric, along with its type
- _Interval_: The time interval over which the metric is aggregated
- _Aggregations_: The distinct used to aggregate the metric
- _Source_: The source table from which the metric is derived
- _Breakouts_: Custom segmentations of the metric

## Contributing

We welcome contributions from the community! Please check out our [contribution guidelines](docs/CONTRIBUTING.md) for more information on how to get involved.

## License

Distributed under the MIT License. See LICENSE for more information
