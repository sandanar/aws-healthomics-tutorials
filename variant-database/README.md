# Creating and Loading Iceberg Tables with PyIceberg and AWS S3Tables

This project demonstrates how to use PyIceberg to connect to an AWS S3Tables catalog, create Iceberg tables for storing genomic variant data, and load VCF/GVCF files into these tables using boto3's standard credential chain.

## Prerequisites

- AWS account with appropriate permissions
- Python 3.8+
- AWS credentials configured through one of the following methods:
  - AWS CLI configuration
  - Environment variables
  - IAM role
  - EC2 instance profile
  - ECS task role

## Installation

Set up a virtual environment and install dependencies:

```bash
# Create a virtual environment
python -m venv venv

# Run the setup script to activate and install dependencies
./setup.sh

# Or manually activate and install
source venv/bin/activate
pip install -r requirements.txt
```

## Configuration

The application uses boto3's standard credential chain, so no explicit credential configuration is needed. Just ensure you have AWS credentials configured through one of the standard methods.

You'll need to update the `BUCKET_ARN` variable in the Python scripts with your S3Tables bucket ARN. For example:

```python
BUCKET_ARN = "arn:aws:s3tables:us-east-1:YOUR_ACCOUNT_ID:bucket/YOUR_BUCKET_NAME"
```

## Required AWS Permissions

The following IAM permissions are required:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3tables:CreateTable",
                "s3tables:GetTable",
                "s3tables:ListNamespaces",
                "s3tables:CreateNamespace",
                "s3tables:ListTables",
                "s3tables:DeleteTable"
            ],
            "Resource": [
                "arn:aws:s3tables:*:*:bucket/*"
            ]
        }
    ]
}
```

## Project Structure

- `utils.py` - Utility functions for working with S3Tables and Iceberg
- `schema_*.py` - Different schema definitions for genomic variant data tables
- `load_vcf_schema*.py` - Scripts to load VCF/GVCF files into the corresponding schema tables
- `describe_tables.py` - Script to describe existing Iceberg tables in the catalog
- `drop_tables.py` - Script to drop (purge) tables from an Iceberg catalog namespace
- `setup.sh` - Script to set up the virtual environment and install dependencies
- `metadata_schema.py` - Schema definitions for VCF metadata (header block)

## Usage

### Creating Tables

To create tables with one of the genomic variant schemas:

```bash
python schema_1.py --bucket-arn <bucket_arn>  # Creates tables in namespace variant_db
python schema_2.py --bucket-arn <bucket_arn>  # Creates tables in namespace variant_db_2
python schema_3.py --bucket-arn <bucket_arn>  # Creates tables in namespace variant_db_3
python schema_4.py --bucket-arn <bucket_arn>  # Creates tables in namespace variant_db_4
```

### Loading VCF/GVCF Data

To load VCF/GVCF files into the tables:

```bash
# Load data into schema 1 tables
python load_vcf_schema1.py --bucket-arn <bucket_arn> path/to/your/file.vcf.gz ...

# Load data into schema 2 tables
python load_vcf_schema2.py --bucket-arn <bucket_arn> path/to/your/file.vcf.gz ...

# Load data into schema 3 tables
python load_vcf_schema3.py --bucket-arn <bucket_arn> path/to/your/file.vcf.gz ...

# Load data into schema 4 tables
python load_vcf_schema4.py --bucket-arn <bucket_arn> path/to/your/file.vcf.gz ...
```

### Describing Tables

To list and describe all tables in the catalog:

```bash
python describe_tables.py --bucket-arn <bucket_arn>
```

### Dropping Tables

To drop all tables in a namespace:

```bash
python drop_tables.py --bucket-arn <bucket_arn> --namespace <namespace> --confirm
```

## Schema Designs

The project implements four different schema designs for genomic variant data:

### Schema 1 (`schema_1.py`)

A normalized schema with three tables in namespace `variant_db`:

1. **variants** - Contains information about genomic variants
   - `variant_id` (UUID, primary key)
   - `chrom` (String) - Chromosome
   - `pos` (Long) - Position
   - `vcf_id` (String)
   - `ref` (String) - Reference allele
   - `alt` (String) - Alternate allele
   - `qual` (Double) - Quality score
   - `filter` (String)
   - Partitioned by: `chrom`
   - Sorted by: `pos` (ascending)

2. **samples** - Contains information about genomic samples
   - `sample_id` (UUID, primary key)
   - `sample_name` (String)

3. **variant_samples** - Contains genotype information linking variants to samples
   - `variant_id` (UUID)
   - `sample_id` (UUID)
   - `genotype` (String)
   - `attributes` (Map<String, String>)
   - `is_reference_block` (Boolean)
   - Partitioned by: `variant_id_bucket` (32 buckets)
   - Sorted by: `sample_id` (ascending)

### Schema 2 (`schema_2.py`)

A denormalized schema with two tables in namespace `variant_db_2`:

1. **samples** - Contains information about genomic samples
   - `sample_id` (UUID, primary key)
   - `sample_name` (String, required)

2. **variant_regions** - Contains variant information with embedded sample data
   - `chrom` (String, required) - Chromosome
   - `pos` (Long, required) - Position
   - `ref` (String, required) - Reference allele
   - `alt` (String, required) - Alternate allele
   - `qual` (Double) - Quality score
   - `filter` (String)
   - `info` (Map<String, String>) - Additional information
   - `sample_ids` (Map<UUID, String>) - Sample IDs
   - `sample_genotypes` (Map<UUID, String>) - Genotypes by sample ID
   - `is_reference_block` (Boolean)
   - Partitioned by: `chrom` and `pos_bucket` (128 buckets)
   - Sorted by: `pos` (ascending)

### Schema 3 (`schema_3.py`)

A fully denormalized schema with a single table in namespace `variant_db_3`:

1. **genomic_variants** - Contains all variant and sample information
   - `sample_id` (UUID, required)
   - `chrom` (String, required) - Chromosome
   - `pos` (Long, required) - Position
   - `ref` (String, required) - Reference allele
   - `alt` (String, required) - Alternate allele
   - `qual` (Double) - Quality score
   - `filter` (String)
   - `genotype` (String)
   - `info` (Map<String, String>) - Variant information
   - `attributes` (Map<String, String>) - Sample-specific attributes
   - `is_reference_block` (Boolean)
   - Partitioned by: `sample_bucket` (128 buckets)
   - Sorted by: `chrom` and `pos` (ascending)

### Schema 4 (`schema_4.py`)

A normalized schema with three tables in namespace `variant_db_4`:

1. **samples** - Contains information about genomic samples
   - `sample_id` (UUID, primary key)
   - `sample_name` (String, required)

2. **variants** - Contains information about genomic variants
   - `variant_id` (UUID, primary key)
   - `chrom` (String, required) - Chromosome
   - `pos` (Long, required) - Position
   - `vcf_id` (String)
   - `ref` (String, required) - Reference allele
   - `alt` (String, required) - Alternate allele
   - `qual` (Double) - Quality score
   - `filter` (String)
   - `info` (Map<String, String>) - Additional information
   - Partitioned by: `chrom` and `pos_bucket` (128 buckets)

3. **variant_samples** - Contains genotype information linking variants to samples
   - `variant_id` (UUID, required)
   - `sample_id` (UUID, required)
   - `chrom` (String, required)
   - `pos` (Long, required)
   - `genotype` (String, required)
   - `attributes` (Map<String, String>)
   - `is_reference_block` (Boolean)
   - Partitioned by: `chrom` and `pos_bucket` (128 buckets)

## Performance Considerations

Each schema design offers different trade-offs:

- **Schema 1**: Normalized design good for data integrity and minimal redundancy but requires joins for most queries
- **Schema 2**: Partially denormalized for better query performance while maintaining sample data separately
- **Schema 3**: Fully denormalized for fastest single-sample queries but with data duplication, especially for multisample (cohort) VCFs
- **Schema 4**: Partially normalized with additional fields in the join table for optimized genomic region queries

## Resources

- [AWS S3Tables Documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [Boto3 Credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)
