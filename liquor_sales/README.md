Welcome to your new dbt project!

### Prerequisites 

To run this DBT project successfully, you will need the following:
1. Set up your Google Cloud Platform account and make sure you can access the [console](https://console.cloud.google.com/)
2. Install gcloud CLI using instructions [here](https://cloud.google.com/sdk/docs/install)
3. Authenticate GCP (The command is `glcoud auth application-default login`)
4. Go to GCP-IAM and give yourself BigQuery Admin role
5. Update profiles.yml (update '\<YOUR-PROJECT-NAME\>')

### Setting up the DBT structure

The DBT structure has already been created for you, so there is no need to create a new one. FYI, the command for initialising a new dbt structure is `dbt init`.

### Configs

DBT's high learning curve is mostly due to setting up the config. There are some configs that are of interest:

- dbt_project.yml: Contains all the global config that tells dbt how to operate your project. See: https://docs.getdbt.com/reference/dbt_project.yml 
- profiles.yml: If dbt_project points to this profile, it tells dbt how to connect to your data warehouse. Since we are working specifically on bigquery, see https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup
- model config: See models section below

### To verify your DBT project setup

Run:

```
dbt debug
```

This command checks that your DBT project is correctly configured and that the connection to your data warehouse works.

### Seeds 

Seeds folder contains any csv that you want DBT to ingest. See: https://docs.getdbt.com/docs/build/seeds as well as https://docs.getdbt.com/reference/seed-configs to understand how to add specific configuration to seeds. 

Now, to generation the csv ingestion, run:

```
dbt seed
```

### Snapshots

Snapshots folder contains the sql logic where the .sql file will capture the "snapshot" of your dimension tables using the type 2 SCDs as taught in class. The configuration you see in the .sql are based on this: https://docs.getdbt.com/docs/build/snapshots#configure-snapshots-in-versions-18-and-earlier 

Note that there are 2 ways to define specific configurations for snapshots, as seen in this: https://docs.getdbt.com/reference/snapshot-configs . You can can do it directly in the .sql (as seen in this demo) or again, via the `dbt_project.yml`. When in doubt, refer to the doc! 

When you are ready to snapshot your data, do:

```
dbt snapshot
```

You may encounter an error if the 'snapshots' dataset is not created in your BigQuery. If the dataset doesn’t exist, create it manually in BigQuery:

1. Click “+ Add” → "Create Dataset".
2. Enter `snapshots` as the dataset name.
3. Set location = US (to match your dbt_project.yml settings).
4. Click Create.

### Models 

SQL files are stored in models folder and they define your transformation logic. `*.sql` are the logic of your transformations. `*.schema.yml` contains the information of the materialized output based on your sql file. Please read [model properties](https://docs.getdbt.com/reference/model-properties) to understand what to configure in the yml. Some examples covered will be the description of the model's name, model's column as well as model's test. Other useful link includes: https://docs.getdbt.com/docs/build/sql-models

Similar to snapshot, you will see that model's config can be defined in multiple places: 

- dbt_project.yml (which is done in this demo)
- <any_name>.yml aka property file (which is demoed in class). Here, it is the schema.yml
- config block: in the model's .sql itself

Please see: https://docs.getdbt.com/reference/model-configs 

When you are ready to run all the model in your dbt, do:

```
dbt run
```

### Testing

Last but not least, dbt also includes simple testing out of the box. We will cover more complex testing next week. But for now, we include some basic tests in the property yml (schema.yml) that are tied to each model (.sql). 

See: https://docs.getdbt.com/docs/build/data-tests#generic-data-tests 

Note that testing is done after the table is ingested, because dbt is simply running queries in your data warehouse which is simply querying the tables.

```
dbt test
```


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
