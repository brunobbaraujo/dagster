---
title: "Dagster project file reference"
description: "A reference of the files in a Dagster project."
sidebar_position: 300
---

This reference contains details about [default](#default-files) and [configuration](#configuration-files) files in a Dagster project, including:

- File names
- What the files do
- Where files should be located in a project directory
- Resources for structuring your Dagster project

## Default files

The following demonstrates a Dagster project using the default project skeleton, generated by the `dagster project scaffold` command:

```shell
.
├── README.md
├── my_dagster_project
│   ├── __init__.py
│   ├──  assets.py
│   └──  definitions.py
├── my_dagster_project_tests
├── pyproject.toml
├── setup.cfg
├── setup.py
└── tox.ini
```

:::note

While this specific example uses a project created by scaffolding, projects created [using an official example](creating-a-new-project#using-an-official-example) will also contain these files. In official examples, `assets.py` will be a subdirectory instead of a Python file.

:::

Let's take a look at what each of these files and directories does:

### my_dagster_project/ directory

This is a Python module that contains Dagster code. This directory also contains the following:

| File/Directory | Description |
|----------------|-------------|
| `__init__.py`  | A file required in a Python package. For more information, see the [Python documentation](https://docs.python.org/3/reference/import.html#regular-packages). |
| `assets.py` | A Python module that contains asset definitions. **Note:** As your project grows, we recommend organizing assets in sub-modules. For example, you can put all analytics-related assets in a `my_dagster_project/assets/analytics/` directory and use <PyObject section="assets" module="dagster" object="load_assets_from_package_module" /> in the top-level definitions to load them, rather than needing to manually add assets to the top-level definitions every time you define one.<br /><br /> Similarly, you can also use <PyObject section="assets" module="dagster" object="load_assets_from_modules" /> to load assets from single Python files. |
| `definitions.py` | The `definitions.py` file includes a <PyObject section="definitions" module="dagster" object="Definitions" /> object that contains all the definitions defined within your project. A definition can be an asset, a job, a schedule, a sensor, or a resource. This allows Dagster to load the definitions in a module.<br /><br />To learn about other ways to deploy and load your Dagster code, see the [code locations documentation](/deployment/code-locations) |

### my_dagster_project_tests/ directory

A Python module that contains tests for `my_dagster_project`.

### README.md file

A description and starter guide for your Dagster project.

### pyproject.toml file

A file that specifies package core metadata in a static, tool-agnostic way.

This file includes a `tool.dagster` section which references the Python module with your Dagster definitions defined and discoverable at the top level. This allows you to use the `dagster dev` command to load your Dagster code without any parameters. For more information. see the [code locations documentation](/deployment/code-locations).

**Note:** `pyproject.toml` was introduced in [PEP-518](https://peps.python.org/pep-0518/) and meant to replace `setup.py`, but we may still include a `setup.py` for compatibility with tools that do not use this spec.

### setup.py file

A build script with Python package dependencies for your new project as a package. Use this file to specify dependencies.

Note: If using Dagster+, add `dagster-cloud` as a dependency.

### setup.cfg

An ini file that contains option defaults for `setup.py` commands.

## Configuration files

Depending on your use case or if you're using Dagster+, you may also need to add additional configuration files to your project. Refer to the [Example project structures section](#example-project-structures) for a look at how these files might fit into your projects.

| File/Directory | Description | OSS | Dagster+ |
|----------------|-------------|-----|----------|
| dagster.yaml   | Configures your Dagster instance, including defining storage locations, run launchers, sensors, and schedules. For more information. including a list of use cases and available options, see the [`dagster.yaml`](/deployment/oss/dagster-yaml) reference.<br /><br />For [Dagster+ Hybrid deployments](/deployment/dagster-plus/hybrid/), this file can be used to customize the [Hybrid agent](/deployment/dagster-plus/management/customizing-agent-settings). | Optional | Optional |
| dagster_cloud.yaml | Defines code locations for Dagster+. For more information, see the [`dagster_cloud.yaml` reference](/deployment/code-locations/dagster-cloud-yaml). | Not applicable | Recommended |
| deployment_settings.yaml | Configures settings for full deployments in Dagster+, including run queue priority and concurrency limits. Refer to the Deployment settings reference for more info.<br /><br />**Note:** This file can be named anything, but we recommend choosing an easily understandable name. | Not applicable | Optional |
| workspace.yaml | Defines multiple code locations for local development or deploying to your infrastructure. For more information and available options, see the [`workspace.yaml` file reference](/deployment/code-locations/workspace-yaml) | Optional | Not applicable |


## Example project structures

Using the default project skeleton, let's take a look at how some example Dagster projects would be structured in different scenarios.

:::note Configuration file location

With the exception of [`dagster_cloud.yaml`](/deployment/code-locations/dagster-cloud-yaml), it's not necessary for configuration files to be located with your project files. These files typically need to be located in `DAGSTER_HOME`. For example, in larger deployments, `DAGSTER_HOME` and Dagster infrastructure configuration can be managed separately from the code locations they support.

:::

### Local development

<Tabs>
<TabItem value="Single code location">

For local development, a project with a single code location might look like this:

```shell
.
├── README.md
├── my_dagster_project
│   ├── __init__.py
│   ├──  assets.py
│   └──  definitions.py
├── my_dagster_project_tests
├── dagster.yaml      ## optional, used for instance settings
├── pyproject.toml    ## optional, used to define the project as a module
├── setup.cfg
├── setup.py
└── tox.ini
```

</TabItem>
<TabItem value="Multiple code locations">

For local development, a project with multiple code locations might look like this:

```shell
.
├── README.md
├── my_dagster_project
│   ├── __init__.py
│   ├──  assets.py
│   └──  definitions.py
├── my_dagster_project_tests
├── dagster.yaml      ## optional, used for instance settings
├── pyproject.toml
├── setup.cfg
├── setup.py
├── tox.ini
└── workspace.yaml    ## defines multiple code locations
```

</TabItem>
</Tabs>

### Dagster Open Source deployment

Once you're ready to move from working locally to deploying Dagster to your infrastructure, use our [deployment guides](/deployment/oss/deployment-options/) to get up and running.

A Dagster project deployed to your infrastructure might look like this:

```shell
.
├── README.md
├── my_dagster_project
│   ├── __init__.py
│   ├──  assets.py
│   └──  definitions.py
├── my_dagster_project_tests
├── dagster.yaml      ## optional, used for instance settings
├── pyproject.toml
├── setup.cfg
├── setup.py
├── tox.ini
└── workspace.yaml    ## defines multiple code locations
```

### Dagster+

Depending on the type of deployment you're using in Dagster+ - Serverless or Hybrid - your project structure might look slightly different. Click the tabs for more info.

<Tabs>
<TabItem value="Serverless deployment">

#### Serverless deployment

For a Dagster+ Serverless deployment, a project might look like this:

```shell
.
├── README.md
├── my_dagster_project
│   ├── __init__.py
│   ├──  assets.py
│   └──  definitions.py
├── my_dagster_project_tests
├── dagster_cloud.yaml         ## defines code locations
├── deployment_settings.yaml   ## optional, defines settings for full deployments
├── pyproject.toml
├── setup.cfg
├── setup.py
└── tox.ini
```

</TabItem>
<TabItem value="Hybrid deployment">

#### Hybrid deployment

For a Dagster+ Hybrid deployment, a project might look like this:

```shell
.
├── README.md
├── my_dagster_project
│   ├── __init__.py
│   ├──  assets.py
│   └──  definitions.py
├── my_dagster_project_tests
├── dagster.yaml                ## optional, hybrid agent custom configuration
├── dagster_cloud.yaml          ## defines code locations
├── deployment_settings.yaml    ## optional, defines settings for full deployments
├── pyproject.toml
├── setup.cfg
├── setup.py
└── tox.ini
```

</TabItem>
</Tabs>

## Next steps

You learned about the default files in a Dagster project and where they should be located, but what about the files containing your Dagster code?

To sustainably scale your project, check out our best practices and recommendations in the [Structuring your project guide](/guides/build/projects/structuring-your-dagster-project).
