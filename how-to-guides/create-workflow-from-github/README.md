# Creating AWS HealthOmics Workflows from Git Repositories

This guide covers how to create AWS HealthOmics workflows directly from Git repositories using the newly released repository integration feature. This capability allows you to source workflow definitions, parameter templates, and README files directly from version-controlled repositories.

## Overview

AWS HealthOmics now supports creating workflows from Git-based repositories through AWS CodeConnections. This integration enables:

- Direct workflow creation from public and private repositories
- Automatic synchronization with version control
- Support for GitHub, GitLab, Bitbucket, and self-managed instances
- Integration of README files and parameter templates from repositories

## Prerequisites

### 1. AWS Account Setup
- AWS account with appropriate permissions
- AWS CLI configured with credentials
- HealthOmics service permissions configured

### 2. Required IAM Permissions
Your IAM user/role needs the following permissions:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "omics:CreateWorkflow",
                "omics:GetWorkflow",
                "omics:ListWorkflows",
                "codeconnections:CreateConnection",
                "codeconnections:GetConnection",
                "codeconnections:GetHost",
                "codeconnections:ListConnections",
                "codeconnections:UseConnection"
            ],
            "Resource": "*"
        }
    ]
}
```

## Setting Up Code Connections

Before creating workflows from repositories, you must establish a connection to your Git provider.

### Step 1: Create a CodeConnection

#### Using AWS Console:
1. Navigate to the [Developer Tools Console](https://console.aws.amazon.com/codesuite/settings/connections)
2. Choose **Create connection**
3. Select your provider (GitHub, GitLab, Bitbucket, etc.)
4. Follow the provider-specific authentication flow
5. Note the connection ARN for later use

#### Using AWS CLI:
```bash
# Create connection (example for GitHub)
aws codeconnections create-connection \
    --provider-type GitHub \
    --connection-name "my-github-connection"
```

### Step 2: Complete Connection Authorization
After creating the connection, you must authorize it through the provider's OAuth flow. The connection status must be "AVAILABLE" before use.

## Scenario A: Creating Workflows from Public Open-Source Repositories

This scenario covers creating HealthOmics workflows from publicly available workflow definitions on platforms like GitHub.

### Example: Using a Public Workflow

```bash
# Create workflow from public repository
aws omics create-workflow --region us-east-1 \
    --name "public-rnaseq-workflow" \
    --description "RNA-seq analysis workflow from nf-core" \
    --definition-repository '{
        "connectionArn": "arn:aws:codeconnections:us-east-1:592761533288:connection/252682dd-56cc-477e-84c0-57c611cc1d0d",
        "fullRepositoryId": "aws-samples/aws-healthomics-tutorials",
        "sourceReference": {
            "type": "BRANCH",
            "value": "main"
        },
        "excludeFilePatterns": ["notebook/**", "generative-ai/**"]
    }' \
    --main "example-workflows/nf-core/workflows/rnaseq/main.nf" \
    --parameter-template-path "example-workflows/nf-core/workflows/rnaseq/parameter-template.json" \
    --readme-path "example-workflows/nf-core/workflows/rnaseq/README.md" \
    --storage-type "DYNAMIC" \
    --engine "NEXTFLOW"
```


### Key Parameters Explained:

- **connectionArn**: ARN of your CodeConnection
- **fullRepositoryId**: Repository in format "owner/repo-name"
- **sourceReference**: Specific branch, tag, or commit
  - `type`: "BRANCH", "TAG", or "COMMIT"
  - `value`: The specific reference value
- **excludeFilePatterns**: Files/folders to exclude (supports glob patterns)
- **main**: Path to main workflow definition file
- **parameter-template-path**: Path to parameter template JSON within the repo. Parameter templates are optional. If not provided they will be infered by HealthOmics at workflow creation time.
- **readme-path**: Path to README file within repo. Note that `--readme-path` is optional. If not provided, HealthOmics will use `README.md` if it is found at the top level of the repository.

### Using Console for Public Repositories:

1. Open HealthOmics Console → **Private workflows** → **Create workflow**
2. In **Workflow definition source**, select **Import from a repository service**
3. Choose your connection
4. Enter repository ID: `nf-core/rnaseq`
5. Set source reference: Tag `3.12.0`
6. Configure exclude patterns: `tests/`, `*.jpeg`, `docs/`
7. Set main file path: `main.nf`
8. Configure README and parameter template paths
9. Complete workflow creation

## Scenario B: Creating Workflows from Private Repositories

This scenario covers creating workflows from your own private repositories where you maintain custom workflow definitions.

### Example Repository Structure:
```
my-genomics-workflows/
├── workflows/
│   ├── variant-calling/
│   │   ├── main.wdl
│   │   └── tasks/
│   └── rna-seq/
│       ├── main.nf
│       └── modules/
├── parameters/
│   ├── variant-calling-params.json
│   └── rna-seq-params.json
├── docs/
│   ├── variant-calling-README.md
│   └── rna-seq-README.md
└── README.md
```

### Creating Workflow from Private Repository:

```bash
# Create workflow from private repository
aws omics create-workflow \
    --name "custom-variant-calling" \
    --description "Custom variant calling pipeline" \
    --engine "WDL" \
    --definition-repository '{
        "connectionArn": "arn:aws:codeconnections:us-east-1:123456789012:connection/abcd1234-5678-90ab-cdef-1234567890ab",
        "fullRepositoryId": "myorg/my-genomics-workflows",
        "sourceReference": {
            "type": "BRANCH",
            "value": "main"
        },
        "excludeFilePatterns": ["tests/**", "*.log"]
    }' \
    --main "workflows/variant-calling/main.wdl" \
    --parameter-template-path "parameters/variant-calling-params.json" \
    --readme-path "docs/variant-calling-README.md" \
    --storage-type "DYNAMIC" \
```

### Advanced Configuration with Container Registry Mapping:

```bash
# Create workflow with container registry mappings
aws omics create-workflow \
    --name "custom-workflow-with-containers" \
    --definition-repository '{
        "connectionArn": "arn:aws:codeconnections:us-east-1:123456789012:connection/abcd1234-5678-90ab-cdef-1234567890ab",
        "fullRepositoryId": "myorg/my-workflows",
        "sourceReference": {
            "type": "COMMIT",
            "value": "abc123def456"
        }
    }' \
    --main "workflow.wdl" \
    --container-registry-map '{
        "registryMappings": [
            {
                "upstreamRegistryUrl": "registry-1.docker.io",
                "ecrRepositoryPrefix": "docker-hub"
            }
        ],
        "imageMappings": [
            {
                "sourceImage": "docker.io/library/ubuntu:latest",
                "destinationImage": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-ubuntu:latest"
            }
        ]
    }'
```

## README File Integration

### Supported README Sources:
1. **From Repository**: Use `--readme-path` to specify path within repo
2. **From S3**: Use `--readme-uri` for S3-hosted README files
3. **Inline**: Use `--readme-markdown` for direct markdown content

### Example README Integration:
```bash
# Using README from repository
aws omics create-workflow \
    --name "documented-workflow" \
    --definition-repository '...' \
    --readme-path "docs/workflow-guide.md"

# Using README from S3
aws omics create-workflow \
    --name "s3-readme-workflow" \
    --definition-repository '...' \
    --readme-uri "s3://my-bucket/workflow-docs/readme.md"
```

## Best Practices

### 1. Repository Organization
- Keep workflow definitions in dedicated directories or consider using one repository per workflow
- Separate parameter templates from workflow code
- Use clear naming conventions for main workflow files
- Include comprehensive README files

### 2. Version Control Strategy
- Use semantic versioning for workflow releases
- Tag stable versions for production use
- Use branches for development and testing
- Document changes in commit messages

### 3. Security Considerations
- Use private repositories for proprietary workflows
- Implement proper access controls on repositories
- Exclude sensitive files not required to create the workflow using exclude patterns

### 4. Performance Optimization
- Use exclude patterns to exclude unnecessary files
- Minimize the number of files in the repository
- Configure container registry mappings for faster pulls

## Troubleshooting

### Common Issues:

1. **Connection Not Available**
   ```bash
   # Check connection status
   aws codeconnections get-connection \
       --connection-arn "arn:aws:codeconnections:..."
   ```

2. **Repository Access Denied**
   - Verify repository permissions
   - Check connection authorization status
   - Ensure repository ID format is correct

3. **Workflow Creation Failed**
   - Validate main workflow file path
   - Check exclude patterns syntax
   - Verify parameter template JSON format

4. **Missing Files**
   - Confirm file paths are relative to repository root
   - Check if files are excluded by patterns
   - Verify source reference points to correct branch/tag or commit ID

### Validation Commands:
```bash
# List available connections
aws codeconnections list-connections

# Get workflow status
aws omics get-workflow --id "workflow-id"

# List workflows
aws omics list-workflows
```
